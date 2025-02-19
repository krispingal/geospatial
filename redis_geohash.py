import redis
import uuid
from datetime import datetime
import random

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
MAX_INITIAL_RESULTS = 150  # Set a reasonable limit


def generate_coordinates(base_lat, base_lon, radius_km=5):
    # Crude approximation: 1 degree lat/lng ~= 111km at equator
    # We're not being precise here, just generating sample data
    lat_offset = (random.random() - 0.5) * 2 * radius_km / 111
    lon_offset = (random.random() - 0.5) * 2 * radius_km / 111
    return base_lat + lat_offset, base_lon + lon_offset


def paginate_geosearch(
    key: str,
    lon: float,
    lat: float,
    radius: float,
    unit="km",
    page_size=5,
    page_num=1,
    session_id=None,
):
    if session_id is None:
        session_id = str(uuid.uuid4())

    temp_key = f"temp:{session_id}:key"

    if page_num == 1 and (not r.exists(temp_key) or r.ttl(temp_key) < 60):
        results = r.geosearch(
            "restaurants",
            longitude=lon,
            latitude=lat,
            radius=radius,
            unit=unit,
            withdist=True,
            count=MAX_INITIAL_RESULTS,
            sort="ASC",
        )
        pipe = r.pipeline()
        for member_id, distance in results:
            pipe.zadd(temp_key, {member_id: distance})

        pipe.expire(temp_key, 600)  # set expiration for 10 mins
        pipe.execute()
    start = (page_num - 1) * page_size
    end = start + page_size - 1
    results = r.zrange(temp_key, start, end, withscores=True)
    return session_id, results


if __name__ == "__main__":
    # Clear data
    r.delete("restaurants", "drivers", "customers")
    nyc_lat, nyc_lon = 40.7128, -74.0060
    restaurants = []
    for i in range(40):
        restaurant_id = f"restaurant:{1001 + i}"
        lat, lon = generate_coordinates(nyc_lat, nyc_lon, 7)
        cuisine = random.choice(["Italian", "Mexican", "Chinese", "Indian", "American"])
        rating = 3 + 2 * random.random()

        r.hset(
            f"metadata:{restaurant_id}",
            mapping={
                "name": f"Restaurant {i + 1}",
                "cuisine": cuisine,
                "rating": rating,
                "address": f"{i + 1} Sample Street, NYC",
            },
        )

        r.geoadd("restaurants", (lon, lat, restaurant_id))
        restaurants.append((restaurant_id, lat, lon))
    print(f"Added {len(restaurants)} restaurants to Redis...")

    drivers = []

    for i in range(10):
        driver_id = f"driver:{101 + i}"
        lat, lon = generate_coordinates(nyc_lat, nyc_lon, 10)

        r.hset(
            f"metadata:{driver_id}",
            mapping={
                "name": f"Driver {i}",
                "vehicle": random.choice(["car", "bike", "scooter"]),
                "rating": 3 + 2 * random.random(),
            },
        )

        r.geoadd("drivers", (lon, lat, driver_id))
        drivers.append((driver_id, lat, lon))
    print(f"Added {len(drivers)} drivers to Redis...")

    customers = []

    for i in range(5):
        customer_id = f"customer:{201 + i}"
        lat, lon = generate_coordinates(nyc_lat, nyc_lon, 8)

        r.hset(
            f"metadata:{customer_id}",
            mapping={
                "name": f"Customer {i}",
                "address": f"{random.randint(1, 999)} Customer Avenue, NYC",
                "joined_date": datetime.now().strftime("%Y-%m-%d"),
            },
        )

        r.geoadd("customers", (lon, lat, customer_id))
        customers.append((customer_id, lat, lon))

    print(f"Added {len(customers)} customers to Redis...")

    # Retrieving restaurants in a radius of x around user
    customer_id, customer_lat, customer_lon = customers[0]

    res = r.geosearch(
        "restaurants",
        longitude=customer_lon,
        latitude=customer_lat,
        radius=3,
        unit="km",
        withdist=True,
        sort="ASC",
    )

    print(f"Found {len(res)} restaurants near {customer_id}")
    for rid, dist in res:
        metadata = r.hgetall(f"metadata:{rid}")
        print(f" - {metadata['name']} ({metadata['cuisine']}) - {dist:.2f} km away")

    # Retrieving restaurants in a radius of x around another restaurant
    restaurant_id = restaurants[0][0]

    res = r.geosearch(
        "restaurants",
        member=restaurant_id,
        radius=3,
        unit="km",
        withdist=True,
        sort="ASC",
    )

    print(f"Found {len(res)} restaurants near {restaurant_id}")
    for rid, dist in res:
        metadata = r.hgetall(f"metadata:{rid}")
        print(f" - {metadata['name']} ({metadata['cuisine']}) - {dist:.2f} km away")

    # Retrieving Restaurants from a bounding box centered around customer
    res = r.geosearch(
        "restaurants",
        longitude=customer_lon,
        latitude=customer_lat,
        height=4,
        width=4,
        unit="km",
        withdist=True,
        sort="ASC",
    )

    print(f"Found {len(res)} restaurants in 4x4 km box around {customer_id}:")
    for rid, dist in res:
        metadata = r.hgetall(f"metadata:{rid}")
        print(f" - {metadata['name']} ({metadata['cuisine']}) - {dist:.2f} km away")

    # Pagination for large result sets
    session_id = None
    for i in range(3):
        session_id, res = paginate_geosearch(
            "restaurants", nyc_lon, nyc_lat, 6, page_num=i + 1, session_id=session_id
        )
        print(f"Closest restaurants to NYC {nyc_lat}, {nyc_lon} Page {i + 1} results")
        for rid, dist in res:
            metadata = r.hgetall(f"metadata:{rid}")
            print(f" - {metadata['name']} ({metadata['cuisine']}) - {dist:.2f} km away")

    # Clean up
    r.delete("restaurants", "drivers", "customers")
