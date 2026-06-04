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
    fn drop(&mut self) {
        // Making this blocking call in drop is suboptimal.
        // We should use async-drop, when it's stabilized:
        // https://github.com/rust-lang/rust/issues/126482
        block_on(self.cleanup());
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
    /// # Errors
    ///
    /// No errors are returned, as this is meant to be called from `drop()`.
    /// Traces are emitted though.
    ///
    #[instrument(level = Level::TRACE, skip_all)]
    async fn cleanup(&mut self) -> () {
        // Calling this might not be strictly necessary,
        // but it's probably expected so let's just do it.
        // I don't think this does any networking most of the time.
        let finish_result = self.search_stream.finish().await;

        match finish_result.success() {
            Ok(_) => (), // All good.
            // This is returned if the stream is cancelled in the middle.
            // Which is fine for us.
            // https://ldap.com/ldap-result-code-reference-client-side-result-codes/#rc-userCanceled
            Err(LdapError::LdapResult {
                result: LdapResult { rc: 88, .. },
            }) => (),
            Err(finish_err) => error!("The stream finished with an error: {finish_err}"),
        }

        match self.search_stream.state() {
            // Stream processed to the end, no need to cancel the operation.
            // This should be the common case.
            StreamState::Done | StreamState::Closed => (),
            StreamState::Error => {
                error!(
                    "Stream is in Error state. Not trying to cancel it as it could do more harm than good."
                );
            }
            StreamState::Fresh | StreamState::Active => {
                info!("Stream is still open. Issuing cancellation to the server.");
                let msgid = self.search_stream.ldap_handle().last_id();
                let result = self.search_stream.ldap_handle().abandon(msgid).await;

                match result {
                    Ok(_) => (),
                    Err(err) => {
                        error!("Error abandoning search result: {:?}", err);
                    }
                }
            }
        }
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
            Ok(None) => Ok(None),
            Err(ldap_error) => Err(Error::Query(
                format!("Error getting next record: {ldap_error:?}"),
                ldap_error,
            )),
        }
    });

    Ok(stream)
}
