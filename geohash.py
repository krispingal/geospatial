class Geohash:
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"  # Standard Base32 characters

    def __init__(self, precision: int = 5):
        """Initialize Geohash encoder/decoder with given precision."""
        if not 1 <= precision <= 12:  # 12 is standard max precision
            raise ValueError("Precision must be between 1 and 12")
        self.precision = precision

    def _encode_bitstream(
        self, value: float, lo: float, hi: float, bit_length: int
    ) -> list[int]:
        """Encodes a value into a bitstream using binary subdivision."""
        if not lo <= value <= hi:
            raise ValueError(f"Value {value} must be between {lo} and {hi}")

        res = []
        for _ in range(bit_length):
            mid = (lo + hi) / 2
            if value > mid:
                lo = mid
                res.append(1)
            else:
                hi = mid
                res.append(0)
        return res

    def encode(self, lat: float, lon: float) -> str:
        """Encode a latitude and longitude into a geohash."""
        bit_length = self.precision * 5
        lat_bits = bit_length // 2
        lon_bits = bit_length - lat_bits

        lat_stream = self._encode_bitstream(lat, -90, 90, lat_bits)
        lon_stream = self._encode_bitstream(lon, -180, 180, lon_bits)

        # Interleave bits more safely
        geohash_bits = []
        for i in range(max(lat_bits, lon_bits)):
            if i < lon_bits:
                geohash_bits.append(lon_stream[i])
            if i < lat_bits:
                geohash_bits.append(lat_stream[i])

        # Convert to base32
        result = []
        for i in range(0, bit_length, 5):
            chunk = geohash_bits[i : i + 5]
            # Pad with zeros if needed
            chunk.extend([0] * (5 - len(chunk)))

            value = sum(bit << (4 - j) for j, bit in enumerate(chunk))
            result.append(self.BASE32[value])

        return "".join(result)

    def _decode_bitstream(
        self, bitstream: int, bit_count: int, min_val: float, max_val: float
    ) -> float:
        """Decodes a bitstream back into a value using binary subdivision."""
        for i in range(bit_count):
            mid = (min_val + max_val) / 2
            if (bitstream >> (bit_count - 1 - i)) & 1:
                min_val = mid
            else:
                max_val = mid
        return (min_val + max_val) / 2

    def decode(self, geohash: str) -> tuple[float, float]:
        """Decode a geohash into a latitude and longitude."""
        if len(geohash) != self.precision:
            raise ValueError(
                f"Geohash length {len(geohash)} doesn't match precision {self.precision}"
            )

        if not all(c in self.BASE32 for c in geohash):
            raise ValueError("Invalid character in geohash")

        bit_length = self.precision * 5
        lat_bits = bit_length // 2
        lon_bits = bit_length - lat_bits

        # Convert from base32 to binary
        geohash_value = 0
        for char in geohash:
            geohash_value = (geohash_value << 5) | self.BASE32.index(char)

        # Deinterleave bits
        lat_stream = lon_stream = 0
        for i in range(bit_length):
            bit = (geohash_value >> (bit_length - 1 - i)) & 1
            if i % 2 == 0:
                lon_stream = (lon_stream << 1) | bit
            else:
                lat_stream = (lat_stream << 1) | bit

        lat = self._decode_bitstream(lat_stream, lat_bits, -90, 90)
        lon = self._decode_bitstream(lon_stream, lon_bits, -180, 180)
        return lat, lon

    def _get_cell_size(self, precision: int) -> tuple[float, float]:
        """Calculate the approximate size of a geohash cell for a given precision.

        Args:
            precision (int): precision/length of geohash

        Returns:
            (latitude_error, longitude_error)
        """
        lat_bits = precision * 5 // 2
        lon_bits = precision * 5 - lat_bits

        lat_err = 180.0 / (1 << lat_bits)
        lon_err = 360.0 / (1 << lon_bits)

        return lat_err, lon_err

    def get_neighbors(self, geohash: str) -> dict[str, str]:
        """
        Compute the 8 neighboring geohashes (N, S, E, W, NE, NW, SE, SW).
        """
        center_lat, center_lon = self.decode(geohash)
        lat_height, lon_width = self._get_cell_size(len(geohash))
        directions = {
            "n": (lat_height, 0),
            "s": (-lat_height, 0),
            "e": (0, lon_width),
            "w": (0, -lon_width),
            "ne": (lat_height, lon_width),
            "se": (-lat_height, lon_width),
            "nw": (lat_height, -lon_width),
            "sw": (-lat_height, -lon_width),
        }
        neighbors = {}
        for direction, (dlat, dlon) in directions.items():
            nlat, nlon = center_lat + dlat, center_lon + dlon

            # handle latitude bounds
            if nlat > 90:
                nlat = 90
            elif nlat < -90:
                nlat = -90

            # handle longitude wrapping
            if nlon > 180:
                nlon -= 360
            elif nlon < -180:
                nlon += 360

            neighbors[direction] = self.encode(nlat, nlon)
        return neighbors


if __name__ == "__main__":
    geo = Geohash(precision=6)
    encoded = geo.encode(41.878738, -87.6359612)  # Willis Tower
    decoded = geo.decode(encoded)
    neighbors = geo.get_neighbors(encoded)

    print(f"Encoded: {encoded}")
    print(f"Decoded: {decoded}")
    print(f"Neighbors: {neighbors}")
