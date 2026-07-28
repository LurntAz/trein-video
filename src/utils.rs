use uuid::Uuid;

pub fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

pub fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_mb() {
        assert_eq!(bytes_to_mb(1_000_000), 0.9536743164062500);
    }

    #[test]
    fn test_bytes_to_gb() {
        assert_eq!(bytes_to_gb(1_000_000_000), 0.9313225746154785);
    }
}
