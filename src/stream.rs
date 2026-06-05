//! Native rust streams for streaming searches
//!

use futures::Stream;
use ldap3::{LdapError, LdapResult, SearchEntry, SearchStream, StreamState};
use tokio::{runtime::Handle, task::block_in_place};
use tracing::{Level, debug, error, instrument, warn};

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

    /// Anyway nowadays we do most cleanup in the stream end, so this does nothing.
    /// The risk still exists if the stream is dropped mid way though.
    ///
    /// In this case there might exist a possibility of a futurelock too.
    fn drop(&mut self) {
        match self.search_stream.state() {
            // Avoiding the block if this stream has already been cleaned up.
            StreamState::Closed | StreamState::Error => (),
            StreamState::Fresh | StreamState::Active | StreamState::Done => {
                // Making this blocking call in drop is suboptimal.
                // We should use async-drop, when it's stabilized:
                // https://github.com/rust-lang/rust/issues/126482
                //
                // Previously we used `futures::block_on()` but that ran the risk of deadlocks
                // (not entirely clear why). The client object is already guaranteed to be in a tokio runtime,
                // and so the lifetime `'a` also guarantees that we are now in a tokio managed thread.
                // Thus we can run some async code with block_in_place().
                // This does necessitate that we're in a multithread executor though.
                warn!("Dropping a stream mid way. Performing blocking cleanup in drop().");
                let result = block_in_place(|| {
                    Handle::current().block_on(async move {
                        self.cleanup().await
                    })
                });
                match result {
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
                debug!("Stream is still open. Issuing cancellation to the server.");

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
#[instrument(level = Level::TRACE, skip_all, ret)]
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
                //
                // Actually we cannot call `self.cleanup()` here because that will send
                // unnecessary search abandon if the stream had no adaptors:
                // https://github.com/inejge/ldap3/issues/155
                //
                // Just finishing is okay though.
                let cleanup_result = finish_stream(&mut search.search_stream).await;

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
