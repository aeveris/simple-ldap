//! Native rust streams for streaming searches
//!

use futures::{Stream, executor::block_on};
use ldap3::{LdapError, LdapResult, SearchEntry, SearchStream, StreamState};
use tracing::{Level, error, info, instrument};

use crate::{Error, Record};

/// This wrapper exists solely for the purpose of running some cleanup in `drop()`.
///
/// This should be refactored to implement `AsyncDrop` when it gets stabilized:
/// https://github.com/rust-lang/rust/issues/126482
struct StreamDropWrapper<'a, S, A>
where
    S: AsRef<str> + Send + Sync + 'a,
    A: AsRef<[S]> + Send + Sync + 'a,
{
    pub search_stream: SearchStream<'a, S, A>,
}

impl<'a, S, A> Drop for StreamDropWrapper<'a, S, A>
where
    S: AsRef<str> + Send + Sync + 'a,
    A: AsRef<[S]> + Send + Sync + 'a,
{
    /// ⚠️ Deadlock risk
    ///
    /// Sporadic async deadlocks have been encountered here.
    /// Some version of [Futurelock](https://rfd.shared.oxide.computer/rfd/0609),
    /// I believe it's due to ldap3 storing stream adaptors inside tokio mutexes
    /// in combination with some future structures.
    ///
    /// Anyway nowadays we do most cleanup in the stream end, so this does nothing.
    /// The risk still exists if the stream is dropped mid way though.
    fn drop(&mut self) {
        match self.search_stream.state() {
            // Avoiding the block if this stream has already been cleaned up.
            StreamState::Closed | StreamState::Error => (),
            StreamState::Fresh | StreamState::Active | StreamState::Done => {
                // Making this blocking call in drop is suboptimal.
                // We should use async-drop, when it's stabilized:
                // https://github.com/rust-lang/rust/issues/126482
                match block_on(self.cleanup()) {
                    Ok(()) => (),
                    Err(ldap_err) => {
                        // Cannot return the error from drop but at least we can log it.
                        error!("Error in finishing the stream: {ldap_err}")
                    }
                }
            }
        }
    }
}

impl<'a, S, A> StreamDropWrapper<'a, S, A>
where
    S: AsRef<str> + Send + Sync + 'a,
    A: AsRef<[S]> + Send + Sync + 'a,
{
    ///
    /// Cleanup the stream. This method should be called when dropping the stream.
    ///
    /// This method will cleanup the stream and close the connection.
    ///
    ///
    /// # Error state
    ///
    /// If the stream is already in an error state this won't do anything.
    /// An Ok result is returned.
    #[instrument(level = Level::TRACE, skip_all)]
    async fn cleanup(&mut self) -> Result<(), LdapError> {
        match self.search_stream.state() {
            // `cleanup()` already called when running it to the end.
            // Nothing to here anymore
            StreamState::Closed => Ok(()),
            // Stream ended but not yet closed.
            // Doing it here.
            StreamState::Done => finish_stream(&mut self.search_stream).await,
            StreamState::Error => {
                error!(
                    "Stream is in Error state. Not trying to cancel it as it could do more harm than good."
                );
                // We don't have an LdapError to return here.
                Ok(())
            }
            StreamState::Fresh | StreamState::Active => {
                info!("Stream is still open. Issuing cancellation to the server.");

                // Let's first call finish according to the docs.
                // This probably wont do too much but it gives the adapters a chance for some cleanup.
                finish_stream(&mut self.search_stream).await?;

                // Then the actual cancellation.
                let msgid = self.search_stream.ldap_handle().last_id();
                self.search_stream.ldap_handle().abandon(msgid).await
            }
        }
    }
}

/// Just a DRY helper for calling `finish()` on the stream.
async fn finish_stream<'a, S, A>(stream: &mut SearchStream<'a, S, A>) -> Result<(), LdapError>
where
    S: AsRef<str> + Send + Sync + 'a,
    A: AsRef<[S]> + Send + Sync + 'a,
{
    // Calling this might not be strictly necessary,
    // but it's probably expected so let's just do it.
    // I don't think this does any networking most of the time.
    let finish_result = stream.finish().await;

    match finish_result.success() {
        Ok(_) => Ok(()), // All good.
        // This is returned if the stream is cancelled in the middle.
        // Which is fine for us.
        // https://ldap.com/ldap-result-code-reference-client-side-result-codes/#rc-userCanceled
        Err(LdapError::LdapResult {
            result: LdapResult { rc: 88, .. },
        }) => Ok(()),
        Err(finish_err) => Err(finish_err),
    }
}

/// A helper to create native rust streams out of `ldap3::SearchStream`s.
pub(crate) fn to_native_stream<'a, S, A>(
    ldap3_stream: SearchStream<'a, S, A>,
) -> Result<impl Stream<Item = Result<Record, Error>> + 'a + use<'a, S, A>, Error>
where
    S: AsRef<str> + Send + Sync + 'a,
    A: AsRef<[S]> + Send + Sync + 'a,
{
    // This will handle stream cleanup.
    let stream_wrapper = StreamDropWrapper {
        search_stream: ldap3_stream,
    };

    // Produce the steam itself by unfolding.
    let stream = futures::stream::try_unfold(stream_wrapper, async |mut search| {
        match search.search_stream.next().await {
            // In the middle of the stream. Produce the next result.
            Ok(Some(result_entry)) => Ok(Some((
                Record {
                    search_entry: SearchEntry::construct(result_entry),
                },
                search,
            ))),
            // Stream is done.
            Ok(None) => {
                // Performing the cleanup here before yielding the end of the stream.
                // This is nice place for this as we're already in an async context.
                // The alternative is to block on this in `drop()`.
                // That still has to be called because streams may be dropped mid way too,
                // but running them to completion is assumed to be the common case.
                let cleanup_result = search.cleanup().await;

                // Doing the cleanup here (as opposed to drop) also has the advantage that we can
                // return the potential error.
                match cleanup_result {
                    Ok(()) => Ok(None),
                    Err(ldap_err) => {
                        Err(Error::Query(String::from("Error finishing the streaming search"), ldap_err))
                    }
                }
            },
            Err(ldap_error) => Err(Error::Query(
                format!("Error getting next record: {ldap_error:?}"),
                ldap_error,
            )),
        }
    });

    Ok(stream)
}
