use aws_smithy_async::future::pagination_stream::PaginationStream;
use futures::stream;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

/// Converts [`Self`] into [`Stream`].
pub trait IntoStream {
    type Item;

    fn into_stream(self) -> Pin<Box<dyn Stream<Item = Self::Item> + Send>>;
}

/// Converts [`PaginationStream`] into [`Stream`].
impl<T> IntoStream for PaginationStream<T>
where
    T: Send + Unpin + 'static,
{
    type Item = T;

    /// Uses [`unfold`](https://docs.rs/futures/0.3.15/futures/stream/fn.unfold.html)
    /// to convert [`PaginationStream`] into [`Stream`].
    fn into_stream(self) -> Pin<Box<dyn Stream<Item = Self::Item> + Send + 'static>> {
        stream::unfold(self, |mut state| async move {
            state.next().await.map(|item| (item, state))
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_async::future::pagination_stream::fn_stream::FnStream;

    #[tokio::test]
    async fn test_into_stream() {
        let items = vec![1, 2, 3, 4, 5];
        let pagination_stream = PaginationStream::new(FnStream::new(|tx| {
            Box::pin(async move {
                for i in items {
                    tx.send(i).await.unwrap();
                }
            })
        }));

        let stream = pagination_stream.into_stream();

        let result: Vec<_> = stream.collect().await;

        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }
}
