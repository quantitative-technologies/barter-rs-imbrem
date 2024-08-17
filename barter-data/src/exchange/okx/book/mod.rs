use crate::{
    event::{MarketEvent, MarketIter},
    exchange::ExchangeId,
    subscription::book::{Level, OrderBookL1},
};

use super::message::OkxMessage;
use barter_integration::model::Exchange;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terse type alias for an [`Okx`](super::super::Kraken) real-time OrderBook Level1
/// (top of book) WebSocket message.
pub type OkxOrderBook = OkxMessage<OkxOrderBookInner>;

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OkxOrderBookInner {
    asks: Vec<OxkLevel>,
    bids: Vec<OxkLevel>,
    #[serde(deserialize_with = "barter_integration::de::de_str_f64_epoch_ms_as_datetime_utc")]
    ts: DateTime<Utc>,
    checksum: Option<i64>,
    #[serde(rename = "seqId")]
    seq_id: i64,
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OxkLevel {
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub deprecated: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub no_orders: f64,
}

impl From<OxkLevel> for Level {
    fn from(level: OxkLevel) -> Self {
        Level {
            price: level.price,
            amount: level.amount,
        }
    }
}

impl<InstrumentId: Clone> From<(ExchangeId, InstrumentId, OkxOrderBook)>
    for MarketIter<InstrumentId, OrderBookL1>
{
    fn from((exchange_id, instrument, book): (ExchangeId, InstrumentId, OkxOrderBook)) -> Self {
        book
            .data
            .into_iter()
            .map(|mut book| {
                Ok(MarketEvent {
                    exchange_time: book.ts,
                    received_time: Utc::now(),
                    exchange: Exchange::from(exchange_id),
                    instrument: instrument.clone(),
                    kind: OrderBookL1 {
                        last_update_time: book.ts, //TODO: fix me...
                        // TODO: give actual errors...
                        best_bid: book.bids.pop().unwrap().into(),
                        best_ask: book.asks.pop().unwrap().into(),
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;
        use barter_integration::{
            de::datetime_utc_from_epoch_duration, error::SocketError, model::SubscriptionId,
        };
        use std::time::Duration;

        #[test]
        fn test_okx_message_order_book_l1() {
            struct TestCase {
                input: &'static str,
                expected: Result<OkxOrderBook, SocketError>,
            }

            let tests = vec![
                TestCase {
                    input: r#"
                        {
                            "arg": {
                            "channel": "books",
                            "instId": "BTC-USDT"
                            },
                            "action": "snapshot",
                            "data": [
                            {
                                "asks": [["8476.98", "415", "0", "13"]],
                                "bids": [["8476.97", "256", "0", "12"]],
                                "ts": "1597026383085",
                                "checksum": -855196043,
                                "prevSeqId": -1,
                                "seqId": 123456
                            }
                            ]
                        }
                        "#,
                    expected: Ok(OkxOrderBook {
                        subscription_id: SubscriptionId::from("books|BTC-USDT"),
                        data: vec![OkxOrderBookInner {
                            asks: vec![OxkLevel {
                                price: 8476.98,
                                amount: 415.0,
                                deprecated: 0.0,
                                no_orders: 13.0,
                            }],
                            bids: vec![OxkLevel {
                                price: 8476.97,
                                amount: 256.0,
                                deprecated: 0.0,
                                no_orders: 12.0,
                            }],
                            ts: datetime_utc_from_epoch_duration(Duration::from_millis(
                                1_597_026_383_085,
                            )),
                            checksum: Some(-855_196_043),
                            seq_id: 123_456,
                        }],
                    }),
                },
                TestCase {
                    input: r#"
                        {
                            "arg": {
                            "channel": "books",
                            "instId": "BTC-USDT"
                            },
                            "action": "snapshot",
                            "data": [
                            {      
                                "asks": [
                                    ["8476.98", "415", "0", "13"],
                                    ["8477", "7", "0", "2"],
                                    ["8477.34", "85", "0", "1"],
                                    ["8477.56", "1", "0", "1"],
                                    ["8505.84", "8", "0", "1"],
                                    ["8506.37", "85", "0", "1"],
                                    ["8506.49", "2", "0", "1"],
                                    ["8506.96", "100", "0", "2"]
                                ],
                                "bids": [
                                    ["8476.97", "256", "0", "12"],
                                    ["8475.55", "101", "0", "1"],
                                    ["8475.54", "100", "0", "1"],
                                    ["8475.3", "1", "0", "1"],
                                    ["8447.32", "6", "0", "1"],
                                    ["8447.02", "246", "0", "1"],
                                    ["8446.83", "24", "0", "1"],
                                    ["8446", "95", "0", "3"]
                                ],
                                "ts": "1597026383085",
                                "checksum": -855196043,
                                "prevSeqId": -1,
                                "seqId": 123456
                            }
                            ]
                        }
                        "#,
                    expected: Ok(OkxOrderBook {
                        subscription_id: SubscriptionId::from("books|BTC-USDT"),
                        data: vec![OkxOrderBookInner {
                            asks: vec![
                                OxkLevel {
                                    price: 8476.98,
                                    amount: 415.0,
                                    deprecated: 0.0,
                                    no_orders: 13.0,
                                },
                                OxkLevel {
                                    price: 8477.0,
                                    amount: 7.0,
                                    deprecated: 0.0,
                                    no_orders: 2.0,
                                },
                                OxkLevel {
                                    price: 8477.34,
                                    amount: 85.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8477.56,
                                    amount: 1.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8505.84,
                                    amount: 8.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8506.37,
                                    amount: 85.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8506.49,
                                    amount: 2.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8506.96,
                                    amount: 100.0,
                                    deprecated: 0.0,
                                    no_orders: 2.0,
                                },
                            ],
                            bids: vec![
                                OxkLevel {
                                    price: 8476.97,
                                    amount: 256.0,
                                    deprecated: 0.0,
                                    no_orders: 12.0,
                                },
                                OxkLevel {
                                    price: 8475.55,
                                    amount: 101.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8475.54,
                                    amount: 100.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8475.3,
                                    amount: 1.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8447.32,
                                    amount: 6.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8447.02,
                                    amount: 246.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8446.83,
                                    amount: 24.0,
                                    deprecated: 0.0,
                                    no_orders: 1.0,
                                },
                                OxkLevel {
                                    price: 8446.0,
                                    amount: 95.0,
                                    deprecated: 0.0,
                                    no_orders: 3.0,
                                },
                            ],
                            ts: datetime_utc_from_epoch_duration(Duration::from_millis(
                                1_597_026_383_085,
                            )),
                            checksum: Some(-855_196_043),
                            seq_id: 123_456,
                        }],
                    }),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<OkxOrderBook>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!("TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n");
                    }
                }
            }
        }
    }
}
