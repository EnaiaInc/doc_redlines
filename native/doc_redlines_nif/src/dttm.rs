use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dttm {
    pub raw: u32,
}

impl Dttm {
    pub fn from_raw(raw: u32) -> Option<Self> {
        if raw == 0 { None } else { Some(Self { raw }) }
    }

    pub fn minute(self) -> u32 {
        self.raw & 0x3F
    }

    pub fn hour(self) -> u32 {
        (self.raw >> 6) & 0x1F
    }

    pub fn day(self) -> u32 {
        (self.raw >> 11) & 0x1F
    }

    pub fn month(self) -> u32 {
        (self.raw >> 16) & 0x0F
    }

    pub fn year(self) -> i32 {
        ((self.raw >> 20) & 0x1FF) as i32 + 1900
    }

    pub fn to_naive_datetime(self) -> Option<NaiveDateTime> {
        let year = self.year();
        let month = self.month();
        let day = self.day();
        let hour = self.hour();
        let minute = self.minute();

        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let time = NaiveTime::from_hms_opt(hour, minute, 0)?;
        Some(NaiveDateTime::new(date, time))
    }

    pub fn to_iso8601(self) -> Option<String> {
        self.to_naive_datetime()
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
    }

    pub fn compatible_with(self, other: Self) -> bool {
        match (self.to_naive_datetime(), other.to_naive_datetime()) {
            (Some(left), Some(right)) => {
                let delta = (left - right).num_minutes().abs();
                delta <= 1
            }
            _ => true,
        }
    }

    /// Return whichever Dttm is earlier (min).  Falls back to `self` on
    /// parse failure.
    pub fn min(self, other: Self) -> Self {
        match (self.to_naive_datetime(), other.to_naive_datetime()) {
            (Some(a), Some(b)) if b < a => other,
            _ => self,
        }
    }
}

pub fn timestamps_compatible(left: Option<Dttm>, right: Option<Dttm>) -> bool {
    match (left, right) {
        (None, _) | (_, None) => true,
        (Some(a), Some(b)) => a.compatible_with(b),
    }
}

#[cfg(test)]
mod tests {
    use super::{Dttm, timestamps_compatible};

    #[test]
    fn timestamps_within_one_minute_are_compatible() {
        let a = Dttm::from_raw(0b000_001100100_0001_00001_01010_000001).unwrap();
        let b = Dttm::from_raw(0b111_001100100_0001_00001_01010_000010).unwrap();
        assert!(a.compatible_with(b));
    }

    #[test]
    fn missing_timestamp_is_wildcard() {
        let a = Dttm::from_raw(0x4E2A0C01);
        assert!(timestamps_compatible(a, None));
        assert!(timestamps_compatible(None, a));
    }
}
