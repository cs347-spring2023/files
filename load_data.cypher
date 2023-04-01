LOAD CSV WITH HEADERS FROM "https://storage.googleapis.com/cs327e-open-access-2/airbnb/listings.csv" AS row
WITH row WHERE row.id IS NOT NULL
CREATE (l:Listing {listing_id: row.id})
SET l.name = row.name,
    l.price = toFloat(substring(row.price, 1)),
    l.weekly_price = toFloat(substring(row.weekly_price, 1)),
    l.cleaning_fee = toFloat(substring(row.cleaning_fee, 1)),
    l.property_type = row.property_type,
    l.accommodates = toInteger(row.accommodates),
    l.bedrooms = toInteger(row.bedrooms),
    l.bathrooms = toInteger(row.bathrooms),
    l.availability_365 = toInteger(row.availability_365);
    
MATCH (l:Listing) RETURN COUNT(l);
CREATE INDEX ON :Listing(listing_id);

CREATE CONSTRAINT ON (a:Amenity) ASSERT a.name IS UNIQUE;

LOAD CSV WITH HEADERS FROM "https://storage.googleapis.com/cs327e-open-access-2/airbnb/listings.csv" AS row
WITH row WHERE row.id IS NOT NULL
MATCH (l:Listing {listing_id: row.id})
WITH l, split(replace(replace(replace(row.amenities, "{", ""), "}", ""), "\"", ""), ",") AS amenities
UNWIND amenities AS amenity
MERGE (a:Amenity {name: amenity})
MERGE (l)-[:HAS]->(a);

MATCH (a:Amenity) RETURN COUNT(a);
CREATE CONSTRAINT ON (n:Neighborhood) ASSERT n.neighborhood_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "https://storage.googleapis.com/cs327e-open-access-2/airbnb/listings.csv" AS row
WITH row WHERE row.id IS NOT NULL
MATCH (l:Listing {listing_id: row.id})
MERGE (n:Neighborhood {neighborhood_id: coalesce(row.neighbourhood_cleansed, "NA")})
ON CREATE SET n.name = row.neighbourhood
MERGE (l)-[:IN_NEIGHBORHOOD]->(n);

MATCH (n:Neighborhood) RETURN COUNT(n);
CREATE CONSTRAINT ON (h:Host) ASSERT h.host_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM "https://storage.googleapis.com/cs327e-open-access-2/airbnb/listings.csv" AS row
WITH row WHERE row.host_id IS NOT NULL
MERGE (h:Host {host_id: row.host_id})
ON CREATE SET h.name      = row.host_name,
              h.about     = row.host_abot,
              h.superhost = CASE WHEN row.host_is_super_host = "t" THEN True ELSE False END,
              h.location  = row.host_location,
              h.image     = row.host_picture_url
WITH row, h
MATCH (l:Listing {listing_id: row.id})
MERGE (h)-[:HOSTS]->(l);

MATCH (h:Host) RETURN COUNT(h);
CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE;

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "https://storage.googleapis.com/cs327e-open-access-2/airbnb/reviews.csv" AS row

// User
MERGE (u:User {user_id: row.reviewer_id})
SET u.name = row.reviewer_name

// Review
CREATE (r:Review {review_id: row.id})
SET r.date     = row.date,
    r.comments = row.comments
WITH row, u, r
MATCH (l:Listing {listing_id: row.listing_id})
MERGE (u)-[:WROTE]->(r)
MERGE (r)-[:REVIEWS]->(l);

MATCH (u:User) RETURN COUNT(u);
MATCH (r:Review) RETURN COUNT(r);