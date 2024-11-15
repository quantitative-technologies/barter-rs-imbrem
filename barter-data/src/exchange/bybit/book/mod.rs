use std::collections::HashMap;

use crate::{
    event::{MarketEvent, MarketIter}, exchange::{ExchangeId, ExchangeServer, StreamSelector}, subscription::book::{Level, OrderBookL1, OrderBooksL2}, transformer::book::MultiBookTransformer, ExchangeWsStream
};

use super::{message::BybitPayload, Bybit};
use barter_integration::model::{instrument::Instrument, Exchange, SubscriptionId};
use chrono::Utc;
use futures::future::Lazy;
use l2::BybitBookUpdater;
//use l1::BybitOrderBookL1;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Level 1 OrderBook types.
//pub mod l1;

/// Level 2 OrderBook types.
pub mod l2;

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize, Default)]
// pub struct BybitOrderBookInner {
//     s: String,
//     b: Vec<BybitLevel>,
//     a: Vec<BybitLevel>,
//     u: i64,
//     seq: i64,
// }
pub struct BybitOrderBookInner {
    // #[serde(
    //     alias = "s",
    //     deserialize_with = "super::book::l2::de_ob_l2_subscription_id"
    // )]
    // pub subscription_id: SubscriptionId,
    #[serde(alias = "b")]
    pub bids: Vec<BybitLevel>,
    #[serde(alias = "a")]
    pub asks: Vec<BybitLevel>,
    #[serde(alias = "u")]
    pub update_id: i64,
    #[serde(alias = "seq")]
    pub seq: i64,
}

impl BybitOrderBookInner {
    /// Creates a new BybitOrderBookInner with empty bids and asks
    pub fn new(update_id: i64, seq: i64) -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            update_id,
            seq,
        }
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BybitLevel {
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
}

impl From<BybitLevel> for Level {
    fn from(level: BybitLevel) -> Self {
        Level {
            price: level.price,
            amount: level.amount,
        }
    }
}


// impl<InstrumentId: Clone> From<(ExchangeId, InstrumentId, BybitOrderBookL1)>
//     for MarketIter<InstrumentId, OrderBookL1>
// {
//     fn from(
//         (exchange_id, instrument, mut book): (ExchangeId, InstrumentId, BybitOrderBookL1),
//     ) -> Self {
//         debug!("Bybit OrderBookL1: {book:?}");
        
//         let key = (exchange_id.clone(), instrument.clone());
        
//         // Get current cached values (if they exist)
//         // let (cached_bid, cached_ask) = ORDERBOOK_CACHE
//         //     .lock()
//         //     .unwrap()
//         //     .get(&key)
//         //     .cloned()
//         //     .unwrap_or_default();
        
//         // // Use new values if available, otherwise fall back to cached values
//         //let best_bid = book.data.b.pop().map(Level::from).unwrap_or(cached_bid);
//         //let best_ask = book.data.a.pop().map(Level::from).unwrap_or(cached_ask);
//         let best_bid = Level::default();
//         let best_ask = Level::default();

//         // // Update cache with latest values
//         // if let Ok(mut cache) = ORDERBOOK_CACHE.lock() {
//         //     cache.insert(key, (best_bid.clone(), best_ask.clone()));
//         // }

//         [Ok(MarketEvent {
//             exchange_time: book.time,
//             received_time: Utc::now(),
//             exchange: Exchange::from(exchange_id),
//             instrument: instrument.clone(),
//             kind: OrderBookL1 {
//                 last_update_time: book.time,
//                 best_bid,
//                 best_ask,
//             },
//         })]
//         .into_iter()
//         .collect()
//     }
// }

// #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
// pub struct BybitOrderBookSnapshot(BybitPayload<BybitOrderBookInner>);

// #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
// pub struct BybitOrderBookDelta(BybitPayload<BybitOrderBookInner>);

// // Optional: Add implementations to delegate to the inner type
// impl std::ops::Deref for BybitOrderBookSnapshot {
//     type Target = BybitPayload<BybitOrderBookInner>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl std::ops::Deref for BybitOrderBookDelta {
//     type Target = BybitPayload<BybitOrderBookInner>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;
        use barter_integration::{
            de::datetime_utc_from_epoch_duration, error::SocketError, model::SubscriptionId,
        };
        use std::time::Duration;

        //#[test]
        // fn test_okx_message_order_book_l1() {
        //     struct TestCase {
        //         input: &'static str,
        //         expected: Result<BybitOrderBookL1, SocketError>,
        //     }

        //     let tests = vec![TestCase {
        //         input: r#"
        //                     {
        //                         "topic": "orderbook.50.BTCUSDT",
        //                         "type": "snapshot",
        //                         "ts": 1672304484978,
        //                         "data": {
        //                             "s": "BTCUSDT",
        //                             "b": [
        //                                 [
        //                                     "16493.50",
        //                                     "0.006"
        //                                 ],
        //                                 [
        //                                     "16493.00",
        //                                     "0.100"
        //                                 ]
        //                             ],
        //                             "a": [
        //                                 [
        //                                     "16611.00",
        //                                     "0.029"
        //                                 ],
        //                                 [
        //                                     "16612.00",
        //                                     "0.213"
        //                                 ]
        //                             ],
        //                             "u": 18521288,
        //                             "seq": 7961638724    
        //                         },
        //                         "cts": 1672304484976
        //                     }
        //                 "#,
        //         expected: Ok(BybitOrderBookL1 {
        //             subscription_id: SubscriptionId::from("orderbook.50|BTCUSDT"),
        //             time: datetime_utc_from_epoch_duration(Duration::from_millis(1672304484978)),
        //             data: BybitOrderBookInner {
        //                 s: "BTCUSDT".to_string(),
        //                 b: vec![
        //                     BybitLevel {
        //                         price: 16493.5,
        //                         amount: 0.006,
        //                     },
        //                     BybitLevel {
        //                         price: 16493.0,
        //                         amount: 0.1,
        //                     },
        //                 ],
        //                 a: vec![
        //                     BybitLevel {
        //                         price: 16611.0,
        //                         amount: 0.029,
        //                     },
        //                     BybitLevel {
        //                         price: 16612.0,
        //                         amount: 0.213,
        //                     },
        //                 ],
        //                 u: 18521288,
        //                 seq: 7961638724,
        //             },
        //             r#type: "snapshot".to_string(),
        //         }),
        //     }];

        //     for (index, test) in tests.into_iter().enumerate() {
        //         let actual = serde_json::from_str::<BybitOrderBookL1>(test.input);
        //         match (actual, test.expected) {
        //             (Ok(actual), Ok(expected)) => {
        //                 assert_eq!(actual, expected, "TC{} failed", index)
        //             }
        //             (Err(_), Err(_)) => {
        //                 // Test passed
        //             }
        //             (actual, expected) => {
        //                 // Test failed
        //                 panic!("TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n");
        //             }
        //         }
        //     }
        // }
    }
}
