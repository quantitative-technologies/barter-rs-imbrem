use async_trait::async_trait;
use barter_integration::{
    model::{instrument::Instrument, Side, SubscriptionId},
    protocol::websocket::WsMessage,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{
    error::DataError,
    exchange::{
        bybit::{channel::BybitChannel, message::{BybitPayload, Delta, Snapshot, ValidateType}},
        subscription::ExchangeSub,
    },
    subscription::book::{Level, OrderBook, OrderBookSide},
    transformer::book::{InstrumentOrderBook, OrderBookUpdater},
    Identifier,
};

use super::{BybitLevel, BybitOrderBookInner};


type BybitOrderBookL2Snapshot = BybitPayload<BybitOrderBookInner, Snapshot>;

impl Identifier<Option<SubscriptionId>> for BybitOrderBookL2Snapshot {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl From<BybitOrderBookL2Snapshot> for OrderBook {
    fn from(snapshot: BybitOrderBookL2Snapshot) -> Self {
        let time = snapshot.time;
        let bids = snapshot.data.bids;
        let asks = snapshot.data.asks;
    
        Self {
            last_update_time: time,
            bids: OrderBookSide::new(Side::Buy, bids),
            asks: OrderBookSide::new(Side::Sell, asks),
        }
    }
}

type BybitOrderBookL2Delta = BybitPayload<BybitOrderBookInner, Delta>;

/// Deserialize a
/// [`BybitSpotOrderBookL2Delta`](super::super::spot::l2::BybitSpotOrderBookL2Delta) or
/// [`BybitFuturesOrderBookL2Delta`](super::super::futures::l2::BybitFuturesOrderBookL2Delta)
/// "s" field (eg/ "BTCUSDT") as the associated [`SubscriptionId`]
///
/// eg/ "@depth@100ms|BTCUSDT"
pub fn de_ob_l2_subscription_id<'de, D>(deserializer: D) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    <&str as Deserialize>::deserialize(deserializer)
        .map(|market| ExchangeSub::from((BybitChannel::ORDER_BOOK_L2, market)).id())
}

// #[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
// pub struct BybitOrderBookL2Delta {
//     #[serde(
//         alias = "s",
//         deserialize_with = "super::super::book::l2::de_ob_l2_subscription_id"
//     )]
//     pub subscription_id: SubscriptionId,
//     #[serde(alias = "U")]
//     pub first_update_id: u64,
//     #[serde(alias = "u")]
//     pub last_update_id: u64,
//     #[serde(alias = "b")]
//     pub bids: Vec<BybitLevel>,
//     #[serde(alias = "a")]
//     pub asks: Vec<BybitLevel>,
// }

impl Identifier<Option<SubscriptionId>> for BybitOrderBookL2Delta {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct BybitBookUpdater {
    pub updates_processed: u64,
    pub last_update_id: i64,
}

impl BybitBookUpdater {
    /// Construct a new BinanceSpot [`OrderBookUpdater`] using the provided last_update_id from
    /// a HTTP snapshot.
    pub fn new() -> Self {
        Self {
            updates_processed: 0,
            last_update_id: 0,
        }
    }

    /// BinanceSpot: How To Manage A Local OrderBook Correctly: Step 5:
    /// "The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1"
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly>
    pub fn is_first_update(&self) -> bool {
        self.updates_processed == 0
    }

    /// BinanceSpot: How To Manage A Local OrderBook Correctly: Step 5:
    /// "The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1"
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly>
    // pub fn validate_first_update(&self, update: &BybitOrderBookL2Delta) -> Result<(), DataError> {
    //     let expected_next_id = self.last_update_id + 1;
    //     if update.first_update_id <= expected_next_id && update.last_update_id >= expected_next_id {
    //         Ok(())
    //     } else {
    //         Err(DataError::InvalidSequence {
    //             prev_last_update_id: self.last_update_id,
    //             first_update_id: update.first_update_id,
    //         })
    //     }
    // }

    pub fn validate_snapshot_update(&self, _snapshot: &BybitOrderBookL2Snapshot) -> Result<(), DataError> {
        // Nothing to validate for snapshot
        Ok(())
    }

    pub fn validate_delta_update(&self, delta: &BybitOrderBookL2Delta) -> Result<(), DataError> {
        // 1. the update_id should not equal 1, which is reserved for the initial snapshot
        if delta.data.update_id == 1 {
            return Err(DataError::InvalidSequence {
                prev_last_update_id: self.last_update_id.max(0) as u64,
                first_update_id: delta.data.update_id.max(0) as u64,
            });
        }
        // 2. If this is not the first update, the update_id should be the next sequence number
        if !self.is_first_update() && delta.data.update_id != self.last_update_id + 1 {
            return Err(DataError::InvalidSequence {
                prev_last_update_id: self.last_update_id.max(0) as u64,
                first_update_id: delta.data.update_id.max(0) as u64,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum BybitBookUpdate {
    #[serde(rename = "snapshot")]
    Snapshot(BybitOrderBookL2Snapshot),
    #[serde(rename = "delta")]
    Delta(BybitOrderBookL2Delta),
}

impl Identifier<Option<SubscriptionId>> for BybitBookUpdate {
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            BybitBookUpdate::Snapshot(snapshot) => snapshot.id(),
            BybitBookUpdate::Delta(delta) => delta.id(),
        }
    }
}

/// Test
//use chrono::Utc;
//use barter_integration::model::Side;

// impl<InstrumentId, Updater> InstrumentOrderBook<InstrumentId, Updater>
// where
//     Updater: Default,
// {
//     /// Creates a new empty InstrumentOrderBook with the provided instrument ID
//     pub fn empty(instrument: InstrumentId) -> Self {
//         Self {
//             instrument,
//             updater: Updater::default(),
//             book: OrderBook {
//                 last_update_time: Utc::now(),
//                 bids: OrderBookSide::new(Side::Buy, vec![]),
//                 asks: OrderBookSide::new(Side::Sell, vec![]),
//             },
//         }
//     }
// }
#[async_trait]
impl OrderBookUpdater for BybitBookUpdater {
    type OrderBook = OrderBook;
    type Update = BybitBookUpdate;

    async fn init<Exchange, Kind>(
        _: mpsc::UnboundedSender<WsMessage>,
        instrument: Instrument,
    ) -> Result<InstrumentOrderBook<Instrument, Self>, DataError>
    where
        Exchange: Send,
        Kind: Send,
    {
        // Empty OrderBook, since there is no initial snapshot yet.
        Ok(InstrumentOrderBook {
            instrument,
            updater: Self::new(),
            book: OrderBook::from(BybitOrderBookL2Snapshot::default()),
        })
    }

    fn update(
        &mut self,
        book: &mut Self::OrderBook,
        update: Self::Update,
    ) -> Result<Option<Self::OrderBook>, DataError> {
        match update {
            BybitBookUpdate::Snapshot(snapshot) => {
                // Replace entire book with snapshot
                self.validate_snapshot_update(&snapshot)?;

                self.last_update_id = snapshot.data.update_id;
                *book = OrderBook::from(snapshot);
                Ok(Some(book.snapshot()))
            }
            BybitBookUpdate::Delta(delta) => {
 
                self.validate_delta_update(&delta)?;

                book.last_update_time = delta.time;
                book.bids.upsert(delta.data.bids);
                book.asks.upsert(delta.data.asks);

                self.last_update_id = delta.data.update_id;
                self.updates_processed += 1;

                Ok(Some(book.snapshot()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod ctor {
        use super::*;

        #[test]
        fn test_construction_and_validation() {
            // let snapshot = BybitOrderBookL2Snapshot::new
            //     subscription_id: SubscriptionId::from("@depth@100ms|ETHUSDT"),
            //     DateTime::<Utc>::from(
            //         std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
            //     ),
            //     "snapshot".to_string(), 
            //     BybitOrderBookInner {
            // );
            let snapshot = BybitOrderBookL2Snapshot {
                subscription_id: SubscriptionId::from("@depth@100ms|ETHUSDT"),
                r#type: "snapshot".to_string(),
                time: DateTime::<Utc>::from(
                    std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                ),
                data: BybitOrderBookInner {
                    update_id: 60559109,
                    bids: vec![
                        BybitLevel {
                            price: 2836.09,
                            amount: 0.51761,
                        },
                        BybitLevel {
                            price: 2835.86,
                            amount: 0.15423,
                        },
                    ],
                    asks: vec![],
                    seq: 71512462685,
                },
                ..Default::default()
            };
            assert!(snapshot.validate_type(), "Snapshot type validation failed");
        }
    }

    mod de {
        use super::*;

        #[test]
        fn test_bybit_order_book_l2_snapshot() {
            struct TestCase {
                input: &'static str,
                expected: BybitBookUpdate,
            }

            let tests = vec![TestCase {
                // TC0: valid Bybit OrderBookL2Snapshot
                input: r#"
                    {
                        "topic": "orderbook.50.ETHUSDT",
                        "ts": 1730955107459,
                        "type": "snapshot",
                        "data": {
                            "s": "ETHUSDT",
                            "b": [
                                ["2836.09", "0.51761"],
                                ["2835.86", "0.15423"],
                                ["2835.84", "0.13661"],
                                ["2835.83", "0.12095"],
                                ["2835.71", "0.03483"],
                                ["2835.68", "0.3526"],
                                ["2835.64", "0.02709"],
                                ["2835.61", "0.25792"],
                                ["2835.6", "0.28117"],
                                ["2835.57", "0.00094"],
                                ["2835.5", "1.57287"],
                                ["2835.49", "0.03708"],
                                ["2835.48", "0.42837"],
                                ["2835.46", "3.5886"],
                                ["2835.45", "1.66"],
                                ["2835.44", "0.03707"],
                                ["2835.41", "0.03486"],
                                ["2835.4", "0.53622"],
                                ["2835.38", "0.70012"],
                                ["2835.37", "0.11534"],
                                ["2835.36", "0.03707"],
                                ["2835.35", "0.13554"],
                                ["2835.34", "0.48606"],
                                ["2835.33", "0.11125"],
                                ["2835.32", "0.06184"],
                                ["2835.3", "0.01593"],
                                ["2835.29", "0.63897"],
                                ["2835.27", "0.04132"],
                                ["2835.24", "0.03715"],
                                ["2835.23", "0.14065"],
                                ["2835.2", "2.3083"],
                                ["2835.19", "1.10422"],
                                ["2835.18", "1.05798"],
                                ["2835.16", "6.77828"],
                                ["2835.12", "16.32594"],
                                ["2835.11", "0.35271"],
                                ["2835.1", "0.05614"],
                                ["2835.08", "0.64923"],
                                ["2835.06", "0.03709"],
                                ["2835.05", "0.49821"],
                                ["2835.03", "1.81531"],
                                ["2835.01", "4.4807"],
                                ["2835", "52.29645"],
                                ["2834.92", "0.79707"],
                                ["2834.9", "0.05229"],
                                ["2834.85", "1.5121"],
                                ["2834.84", "0.44199"],
                                ["2834.83", "1.02541"],
                                ["2834.81", "1.81358"],
                                ["2834.8", "0.56199"]
                            ],
                            "a": [
                                ["2836.1", "0.60857"],
                                ["2836.24", "0.03709"],
                                ["2836.26", "0.06667"],
                                ["2836.29", "0.42129"],
                                ["2836.3", "0.03"],
                                ["2836.36", "0.18"],
                                ["2836.37", "0.35262"],
                                ["2836.38", "0.63785"],
                                ["2836.4", "0.73782"],
                                ["2836.44", "0.53459"],
                                ["2836.45", "0.70125"],
                                ["2836.49", "0.01942"],
                                ["2836.5", "0.06246"],
                                ["2836.52", "0.73897"],
                                ["2836.55", "0.51833"],
                                ["2836.56", "0.44199"],
                                ["2836.58", "0.59643"],
                                ["2836.6", "0.06"],
                                ["2836.62", "0.22636"],
                                ["2836.63", "0.00093"],
                                ["2836.64", "0.26681"],
                                ["2836.68", "4.82671"],
                                ["2836.69", "5.775"],
                                ["2836.7", "0.06"],
                                ["2836.72", "2.21358"],
                                ["2836.75", "8.82"],
                                ["2836.8", "0.06851"],
                                ["2836.81", "0.58613"],
                                ["2836.84", "0.68768"],
                                ["2836.85", "0.2"],
                                ["2836.86", "0.04964"],
                                ["2836.87", "1.60443"],
                                ["2836.88", "2.60805"],
                                ["2836.89", "2.14599"],
                                ["2836.9", "0.06"],
                                ["2836.91", "0.7057"],
                                ["2836.94", "0.1763"],
                                ["2836.97", "1.03508"],
                                ["2837", "0.06246"],
                                ["2837.03", "1.05492"],
                                ["2837.05", "0.70477"],
                                ["2837.06", "0.35262"],
                                ["2837.08", "0.35262"],
                                ["2837.09", "1.21444"],
                                ["2837.1", "1.88624"],
                                ["2837.14", "2.52011"],
                                ["2837.15", "0.62875"],
                                ["2837.19", "0.00093"],
                                ["2837.2", "0.06"],
                                ["2837.23", "4.2424"]
                            ],
                            "u": 60559109,
                            "seq": 71512462685
                        },
                        "cts": 1730955107457
                    }"#,
                expected: BybitBookUpdate::Snapshot(BybitOrderBookL2Snapshot {
                    subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                    r#type: "snapshot".to_string(),
                    time: DateTime::<Utc>::from(
                        std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                    ),
                    data: BybitOrderBookInner {
                        update_id: 60559109,
                        seq: 71512462685,
                        bids: vec![
                            BybitLevel {
                                price: 2836.09,
                                amount: 0.51761,
                            },
                            BybitLevel {
                                price: 2835.86,
                                amount: 0.15423,
                            },
                            BybitLevel {
                                price: 2835.84,
                                amount: 0.13661,
                            },
                            BybitLevel {
                                price: 2835.83,
                                amount: 0.12095,
                            },
                            BybitLevel {
                                price: 2835.71,
                                amount: 0.03483,
                            },
                            BybitLevel {
                                price: 2835.68,
                                amount: 0.3526,
                            },
                            BybitLevel {
                                price: 2835.64,
                                amount: 0.02709,
                            },
                            BybitLevel {
                                price: 2835.61,
                                amount: 0.25792,
                            },
                            BybitLevel {
                                price: 2835.60,
                                amount: 0.28117,
                            },
                            BybitLevel {
                                price: 2835.57,
                                amount: 0.00094,
                            },
                            BybitLevel {
                                price: 2835.50,
                                amount: 1.57287,
                            },
                            BybitLevel {
                                price: 2835.49,
                                amount: 0.03708,
                            },
                            BybitLevel {
                                price: 2835.48,
                                amount: 0.42837,
                            },
                            BybitLevel {
                                price: 2835.46,
                                amount: 3.5886,
                            },
                            BybitLevel {
                                price: 2835.45,
                                amount: 1.66,
                            },
                            BybitLevel {
                                price: 2835.44,
                                amount: 0.03707,
                            },
                            BybitLevel {
                                price: 2835.41,
                                amount: 0.03486,
                            },
                            BybitLevel {
                                price: 2835.40,
                                amount: 0.53622,
                            },
                            BybitLevel {
                                price: 2835.38,
                                amount: 0.70012,
                            },
                            BybitLevel {
                                price: 2835.37,
                                amount: 0.11534,
                            },
                            BybitLevel {
                                price: 2835.36,
                                amount: 0.03707,
                            },
                            BybitLevel {
                                price: 2835.35,
                                amount: 0.13554,
                            },
                            BybitLevel {
                                price: 2835.34,
                                amount: 0.48606,
                            },
                            BybitLevel {
                                price: 2835.33,
                                amount: 0.11125,
                            },
                            BybitLevel {
                                price: 2835.32,
                                amount: 0.06184,
                            },
                            BybitLevel {
                                price: 2835.30,
                                amount: 0.01593,
                            },
                            BybitLevel {
                                price: 2835.29,
                                amount: 0.63897,
                            },
                            BybitLevel {
                                price: 2835.27,
                                amount: 0.04132,
                            },
                            BybitLevel {
                                price: 2835.24,
                                amount: 0.03715,
                            },
                            BybitLevel {
                                price: 2835.23,
                                amount: 0.14065,
                            },
                            BybitLevel {
                                price: 2835.20,
                                amount: 2.3083,
                            },
                            BybitLevel {
                                price: 2835.19,
                                amount: 1.10422,
                            },
                            BybitLevel {
                                price: 2835.18,
                                amount: 1.05798,
                            },
                            BybitLevel {
                                price: 2835.16,
                                amount: 6.77828,
                            },
                            BybitLevel {
                                price: 2835.12,
                                amount: 16.32594,
                            },
                            BybitLevel {
                                price: 2835.11,
                                amount: 0.35271,
                            },
                            BybitLevel {
                                price: 2835.10,
                                amount: 0.05614,
                            },
                            BybitLevel {
                                price: 2835.08,
                                amount: 0.64923,
                            },
                            BybitLevel {
                                price: 2835.06,
                                amount: 0.03709,
                            },
                            BybitLevel {
                                price: 2835.05,
                                amount: 0.49821,
                            },
                            BybitLevel {
                                price: 2835.03,
                                amount: 1.81531,
                            },
                            BybitLevel {
                                price: 2835.01,
                                amount: 4.4807,
                            },
                            BybitLevel {
                                price: 2835.00,
                                amount: 52.29645,
                            },
                            BybitLevel {
                                price: 2834.92,
                                amount: 0.79707,
                            },
                            BybitLevel {
                                price: 2834.90,
                                amount: 0.05229,
                            },
                            BybitLevel {
                                price: 2834.85,
                                amount: 1.5121,
                            },
                            BybitLevel {
                                price: 2834.84,
                                amount: 0.44199,
                            },
                            BybitLevel {
                                price: 2834.83,
                                amount: 1.02541,
                            },
                            BybitLevel {
                                price: 2834.81,
                                amount: 1.81358,
                            },
                            BybitLevel {
                                price: 2834.80,
                                amount: 0.56199,
                            },
                        ],
                        asks: vec![
                            BybitLevel {
                                price: 2836.10,
                                amount: 0.60857,
                            },
                            BybitLevel {
                                price: 2836.24,
                                amount: 0.03709,
                            },
                            BybitLevel {
                                price: 2836.26,
                                amount: 0.06667,
                            },
                            BybitLevel {
                                price: 2836.29,
                                amount: 0.42129,
                            },
                            BybitLevel {
                                price: 2836.30,
                                amount: 0.03,
                            },
                            BybitLevel {
                                price: 2836.36,
                                amount: 0.18,
                            },
                            BybitLevel {
                                price: 2836.37,
                                amount: 0.35262,
                            },
                            BybitLevel {
                                price: 2836.38,
                                amount: 0.63785,
                            },
                            BybitLevel {
                                price: 2836.40,
                                amount: 0.73782,
                            },
                            BybitLevel {
                                price: 2836.44,
                                amount: 0.53459,
                            },
                            BybitLevel {
                                price: 2836.45,
                                amount: 0.70125,
                            },
                            BybitLevel {
                                price: 2836.49,
                                amount: 0.01942,
                            },
                            BybitLevel {
                                price: 2836.50,
                                amount: 0.06246,
                            },
                            BybitLevel {
                                price: 2836.52,
                                amount: 0.73897,
                            },
                            BybitLevel {
                                price: 2836.55,
                                amount: 0.51833,
                            },
                            BybitLevel {
                                price: 2836.56,
                                amount: 0.44199,
                            },
                            BybitLevel {
                                price: 2836.58,
                                amount: 0.59643,
                            },
                            BybitLevel {
                                price: 2836.60,
                                amount: 0.06,
                            },
                            BybitLevel {
                                price: 2836.62,
                                amount: 0.22636,
                            },
                            BybitLevel {
                                price: 2836.63,
                                amount: 0.00093,
                            },
                            BybitLevel {
                                price: 2836.64,
                                amount: 0.26681,
                            },
                            BybitLevel {
                                price: 2836.68,
                                amount: 4.82671,
                            },
                            BybitLevel {
                                price: 2836.69,
                                amount: 5.775,
                            },
                            BybitLevel {
                                price: 2836.70,
                                amount: 0.06,
                            },
                            BybitLevel {
                                price: 2836.72,
                                amount: 2.21358,
                            },
                            BybitLevel {
                                price: 2836.75,
                                amount: 8.82,
                            },
                            BybitLevel {
                                price: 2836.80,
                                amount: 0.06851,
                            },
                            BybitLevel {
                                price: 2836.81,
                                amount: 0.58613,
                            },
                            BybitLevel {
                                price: 2836.84,
                                amount: 0.68768,
                            },
                            BybitLevel {
                                price: 2836.85,
                                amount: 0.2,
                            },
                            BybitLevel {
                                price: 2836.86,
                                amount: 0.04964,
                            },
                            BybitLevel {
                                price: 2836.87,
                                amount: 1.60443,
                            },
                            BybitLevel {
                                price: 2836.88,
                                amount: 2.60805,
                            },
                            BybitLevel {
                                price: 2836.89,
                                amount: 2.14599,
                            },
                            BybitLevel {
                                price: 2836.90,
                                amount: 0.06,
                            },
                            BybitLevel {
                                price: 2836.91,
                                amount: 0.7057,
                            },
                            BybitLevel {
                                price: 2836.94,
                                amount: 0.1763,
                            },
                            BybitLevel {
                                price: 2836.97,
                                amount: 1.03508,
                            },
                            BybitLevel {
                                price: 2837.00,
                                amount: 0.06246,
                            },
                            BybitLevel {
                                price: 2837.03,
                                amount: 1.05492,
                            },
                            BybitLevel {
                                price: 2837.05,
                                amount: 0.70477,
                            },
                            BybitLevel {
                                price: 2837.06,
                                amount: 0.35262,
                            },
                            BybitLevel {
                                price: 2837.08,
                                amount: 0.35262,
                            },
                            BybitLevel {
                                price: 2837.09,
                                amount: 1.21444,
                            },
                            BybitLevel {
                                price: 2837.10,
                                amount: 1.88624,
                            },
                            BybitLevel {
                                price: 2837.14,
                                amount: 2.52011,
                            },
                            BybitLevel {
                                price: 2837.15,
                                amount: 0.62875,
                            },
                            BybitLevel {
                                price: 2837.19,
                                amount: 0.00093,
                            },
                            BybitLevel {
                                price: 2837.20,
                                amount: 0.06,
                            },
                            BybitLevel {
                                price: 2837.23,
                                amount: 4.2424,
                            },
                        ],
                    },
                    ..Default::default()
                })
            }];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<BybitBookUpdate>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), expected) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(err), _) => {
                        panic!("TC{} failed to deserialize: {}", index, err);
                    }
                }
            }
        }
        #[test]        
        fn test_bybit_order_book_l2_delta() {
            struct TestCase {
                input: &'static str,
                expected: BybitBookUpdate,
            }

            let tests = vec![TestCase {
                // TC0: valid Bybit OrderBookL2Delta
                input: r#"
                    {
                        "topic": "orderbook.50.ETHUSDT", 
                        "ts": 1730955107479, 
                        "type": "delta", 
                        "data": {
                            "s": "ETHUSDT", 
                            "b": [
                                ["2835.86", "0.02504"], 
                                ["2835.84", "0"], 
                                ["2835.61", "0"], 
                                ["2835.6", "0"], 
                                ["2835.48", "0"], 
                                ["2835.34", "0.0687"], 
                                ["2835.29", "0.00093"], 
                                ["2835.16", "5.776"], 
                                ["2835.08", "0"], 
                                ["2834.83", "0"], 
                                ["2834.76", "1.05792"], 
                                ["2834.73", "0.00093"], 
                                ["2834.71", "0.03"], 
                                ["2834.7", "3.0072"], 
                                ["2834.68", "1.40538"], 
                                ["2834.67", "4.2424"]
                            ],
                            "a": [
                                ["2836.29", "0.27911"], 
                                ["2836.38", "0.4983"], 
                                ["2836.64", "0"], 
                                ["2836.84", "0"], 
                                ["2836.89", "1.75681"], 
                                ["2836.97", "0"], 
                                ["2837.15", "0"], 
                                ["2837.25", "1.75681"], 
                                ["2837.3", "0.06"], 
                                ["2837.36", "0.27773"], 
                                ["2837.42", "0.21489"]
                            ],
                            "u": 60559110,
                            "seq": 71512462781
                        },
                        "cts": 1730955107476
                    }"#,
                expected: BybitBookUpdate::Delta(BybitOrderBookL2Delta {
                    subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                    r#type: "delta".to_string(),
                    time: DateTime::<Utc>::from(
                        std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107479),
                    ),
                    data: BybitOrderBookInner {
                        update_id: 60559110,
                        seq: 71512462781,
                        bids: vec![
                            BybitLevel { price: 2835.86, amount: 0.02504 },
                            BybitLevel { price: 2835.84, amount: 0.0 },
                            BybitLevel { price: 2835.61, amount: 0.0 },
                            BybitLevel { price: 2835.60, amount: 0.0 },
                            BybitLevel { price: 2835.48, amount: 0.0 },
                            BybitLevel { price: 2835.34, amount: 0.0687 },
                            BybitLevel { price: 2835.29, amount: 0.00093 },
                            BybitLevel { price: 2835.16, amount: 5.776 },
                            BybitLevel { price: 2835.08, amount: 0.0 },
                            BybitLevel { price: 2834.83, amount: 0.0 },
                            BybitLevel { price: 2834.76, amount: 1.05792 },
                            BybitLevel { price: 2834.73, amount: 0.00093 },
                            BybitLevel { price: 2834.71, amount: 0.03 },
                            BybitLevel { price: 2834.70, amount: 3.0072 },
                            BybitLevel { price: 2834.68, amount: 1.40538 },
                            BybitLevel { price: 2834.67, amount: 4.2424 },
                        ],
                        asks: vec![
                            BybitLevel { price: 2836.29, amount: 0.27911 },
                            BybitLevel { price: 2836.38, amount: 0.4983 },
                            BybitLevel { price: 2836.64, amount: 0.0 },
                            BybitLevel { price: 2836.84, amount: 0.0 },
                            BybitLevel { price: 2836.89, amount: 1.75681 },
                            BybitLevel { price: 2836.97, amount: 0.0 },
                            BybitLevel { price: 2837.15, amount: 0.0 },
                            BybitLevel { price: 2837.25, amount: 1.75681 },
                            BybitLevel { price: 2837.30, amount: 0.06 },
                            BybitLevel { price: 2837.36, amount: 0.27773 },
                            BybitLevel { price: 2837.42, amount: 0.21489 },
                        ],
                    },
                    ..Default::default()
                }),
            }];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<BybitBookUpdate>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), expected) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(err), _) => {
                        panic!("TC{} failed to deserialize: {}", index, err);
                    }
                }
            }
        }
    }
    mod bybit_book_updater {
        use super::*;
        use crate::subscription::book::{Level, OrderBookSide};
        use barter_integration::model::Side;

        #[test]
        fn test_is_first_update() {
            struct TestCase {
                input: BybitBookUpdater,
                expected: bool,
            }

            let tests = vec![
                TestCase {
                    // TC0: is first update
                    input: BybitBookUpdater::new(),
                    expected: true,
                },
                TestCase {
                    // TC1: is not first update
                    input: BybitBookUpdater {
                        updates_processed: 10,
                        last_update_id: 100,
                    },
                    expected: false,
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                assert_eq!(
                    test.input.is_first_update(),
                    test.expected,
                    "TC{} failed",
                    index
                );
            }
        }
        #[test]
        fn test_validate_snapshot_update() {
            struct TestCase {
                updater: BybitBookUpdater,
                input: BybitOrderBookL2Snapshot,
                expected: Result<(), DataError>,
            }

            let tests = vec![
                TestCase {
                    // TC0: valid snapshot update
                    updater: BybitBookUpdater::new(),
                    input: BybitOrderBookL2Snapshot {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "snapshot".to_string(),
                        time: DateTime::<Utc>::from(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                        ),
                        data: BybitOrderBookInner {
                            update_id: 60559109,
                            seq: 71512462685,
                            bids: vec![],
                            asks: vec![],
                        },
                        ..Default::default()
                    },
                    expected: Ok(()),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = test.updater.validate_snapshot_update(&test.input);
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
        #[test]
        fn test_validate_delta_update() {
            struct TestCase {
                updater: BybitBookUpdater,
                input: BybitOrderBookL2Delta,
                expected: Result<(), DataError>,
            }

            let tests = vec![
                TestCase {
                    // TC0: invalid delta update with update_id == 1 (reserved for initial snapshot)
                    updater: BybitBookUpdater::new(),
                    input: BybitOrderBookL2Delta {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "delta".to_string(),
                        time: DateTime::<Utc>::from(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                        ),
                        data: BybitOrderBookInner {
                            update_id: 1,
                            seq: 71512462685,
                            bids: vec![],
                            asks: vec![],
                        },
                        ..Default::default()
                    },
                    expected: Err(DataError::InvalidSequence {
                        prev_last_update_id: 0,
                        first_update_id: 1,
                    }),
                },
                TestCase {
                    // TC1: valid first delta update (any update_id except 1)
                    updater: BybitBookUpdater::new(),
                    input: BybitOrderBookL2Delta {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "delta".to_string(),
                        time: DateTime::<Utc>::from(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                        ),
                        data: BybitOrderBookInner {
                            update_id: 2,
                            seq: 71512462685,
                            bids: vec![],
                            asks: vec![],
                        },
                        ..Default::default()
                    },
                    expected: Ok(()),
                },
                TestCase {
                    // TC2: valid non-first delta update (update_id is sequential)
                    updater: BybitBookUpdater {
                        last_update_id: 2,
                        updates_processed: 1,
                    },
                    input: BybitOrderBookL2Delta {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "delta".to_string(),
                        time: DateTime::<Utc>::from(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                        ),
                        data: BybitOrderBookInner {
                            update_id: 3, // Sequential after last_update_id
                            seq: 71512462685,
                            bids: vec![],
                            asks: vec![],
                        },
                        ..Default::default()
                    },
                    expected: Ok(()),
                },
                TestCase {
                    // TC3: invalid non-first delta update (update_id is sequential)
                    updater: BybitBookUpdater {
                        last_update_id: 3,
                        updates_processed: 1,
                    },
                    input: BybitOrderBookL2Delta {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "delta".to_string(),
                        time: DateTime::<Utc>::from(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(1730955107459),
                        ),
                        data: BybitOrderBookInner {
                            update_id: 2, // Not sequential after last_update_id
                            seq: 71512462685,
                            bids: vec![],
                            asks: vec![],
                        },
                        ..Default::default()
                    },
                    expected: Err(DataError::InvalidSequence {
                        prev_last_update_id: 3,
                        first_update_id: 2,
                    }),
                },
             ];

             for (index, test) in tests.into_iter().enumerate() {
                let actual = test.updater.validate_delta_update(&test.input);
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
        #[test]
        fn test_apply_update() {
            struct TestCase {
                updater: BybitBookUpdater,
                book: OrderBook,
                input: BybitBookUpdate,
                expected: Result<Option<OrderBook>, DataError>,
            }

            let time = Utc::now();

            let tests = vec![
                // TC0: snapshot update, overwriting existing book
                TestCase {
                    updater: BybitBookUpdater {
                        updates_processed: 10,
                        last_update_id: 103,
                    },
                    book: OrderBook {
                        last_update_time: time,
                        bids: OrderBookSide::new(Side::Buy, vec![Level::new(50, 1)]),
                        asks: OrderBookSide::new(Side::Sell, vec![Level::new(100, 1)]),
                    },
                    input: BybitBookUpdate::Snapshot(BybitOrderBookL2Snapshot {
                        subscription_id: SubscriptionId::from("orderbook.50|ETHUSDT"),
                        r#type: "snapshot".to_string(),
                        time: time,
                        data: BybitOrderBookInner {
                            update_id: 1,
                            seq: 71512462781,
                            bids: vec![
                                BybitLevel { price: 50.0, amount: 10.0 },
                                BybitLevel { price: 60.0, amount: 20.0 },
                            ],
                            asks: vec![
                                BybitLevel { price: 150.0, amount: 1.0 },
                            ],
                        },
                        ..Default::default()
                    }),
                    expected: Ok(Some(OrderBook {
                        last_update_time: time,
                        bids: OrderBookSide::new(Side::Buy, vec![Level::new(60, 20), Level::new(50, 10)]),
                        asks: OrderBookSide::new(Side::Sell, vec![Level::new(150, 1)]),
                    })),
                },
                TestCase {
                    // TC1: valid update with sorted snapshot generated
                    updater: BybitBookUpdater {
                        updates_processed: 100,
                        last_update_id: 100,
                    },
                    book: OrderBook {
                        last_update_time: time,
                        bids: OrderBookSide::new(
                            Side::Buy,
                            vec![Level::new(80, 1), Level::new(100, 1), Level::new(90, 1)],
                        ),
                        asks: OrderBookSide::new(
                            Side::Sell,
                            vec![Level::new(150, 1), Level::new(110, 1), Level::new(120, 1)],
                        ),      
                    },
                    input: BybitBookUpdate::Delta(BybitOrderBookL2Delta {
                        subscription_id: SubscriptionId::from("subscription_id"),
                        r#type: "delta".to_string(),
                        time: time,
                        data: BybitOrderBookInner {
                            update_id: 101,
                            seq: 71512462781,
                            bids: vec![ 
                                // Level exists & new value is 0 => remove Level
                                BybitLevel {
                                    price: 80.0,
                                    amount: 0.0,
                                },
                                // Level exists & new value is > 0 => replace Level
                                BybitLevel {
                                    price: 90.0,
                                    amount: 10.0,
                                },
                            ],
                            asks: vec![
                                // Level does not exist & new value > 0 => insert new Level
                                BybitLevel {
                                    price: 200.0,
                                    amount: 1.0,
                                },
                                // Level does not exist & new value is 0 => no change
                                BybitLevel {
                                    price: 500.0,
                                    amount: 0.0,
                                },
                            ],
                        },
                        ..Default::default()
                    }),
                    expected: Ok(Some(OrderBook {
                        last_update_time: time,
                        bids: OrderBookSide::new(
                            Side::Buy,
                            vec![Level::new(100, 1), Level::new(90, 10)],
                        ),
                        asks: OrderBookSide::new(
                            Side::Sell,
                            vec![
                                Level::new(110, 1),
                                Level::new(120, 1),
                                Level::new(150, 1),
                                Level::new(200, 1),
                            ],
                        ),
                    })),
                },
            ];

            for (index, mut test) in tests.into_iter().enumerate() {
                let actual = test.updater.update(&mut test.book, test.input);

                match (actual, test.expected) {
                    (Ok(Some(actual)), Ok(Some(expected))) => {
                        // Replace time with deterministic timestamp
                        let actual = OrderBook {
                            last_update_time: time,
                            ..actual
                        };
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Ok(None), Ok(None)) => {
                        // Test passed
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
