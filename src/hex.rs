use anyhow::anyhow;

pub fn parse_hex_string(hex: &str) -> anyhow::Result<Vec<u8>> {
    hex.chars()
        .collect::<Vec<char>>()
        .chunks_exact(2)
        .map(|chunk| {
            let hex_pair: String = chunk.iter().collect();
            u8::from_str_radix(&hex_pair, 16).map_err(|e| anyhow!(e))
        })
        .collect()
}
