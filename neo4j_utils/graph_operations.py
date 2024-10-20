from neo4j import GraphDatabase

class GraphOperations:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def most_purchased_products_by_category(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User)-[:PURCHASED]->(t:Transaction)-[:OF]->(p:Product)
                WHERE t.timestamp > (datetime().epochSeconds - 30 * 24 * 60 * 60)
                WITH p.category AS category, p.productID AS product, COUNT(*) AS purchases
                ORDER BY purchases DESC
                RETURN category, COLLECT({product: product, purchases: purchases})[0..5] AS top_products
            """)
            return result.data()

    def most_influential_users(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User)<-[:FOLLOWS]-(follower:User)
                WITH u, COUNT(follower) AS followers_count
                ORDER BY followers_count DESC
                LIMIT 10
                RETURN u.userID, followers_count
            """)
            return result.data()

    def best_rated_products(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User)-[r:RATED]->(p:Product)
                WITH p, AVG(r.rating_value) AS avg_rating, COLLECT({user: u.userID, rating: r.rating_value}) AS ratings_by_users
                ORDER BY avg_rating DESC
                LIMIT 10
                RETURN p.productID AS product, avg_rating, ratings_by_users
            """)
            return result.data()

    def user_product_clusters(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u1:User)-[:PURCHASED]->(t1:Transaction)-[:OF]->(p:Product)<-[:OF]-(t2:Transaction)<-[:PURCHASED]-(u2:User)
                WHERE u1 <> u2
                WITH u1, u2, COUNT(p) AS sharedProducts
                WHERE sharedProducts > 1
                RETURN u1.userID AS user1, u2.userID AS user2, sharedProducts
                ORDER BY sharedProducts DESC
                LIMIT 10
            """)
            return result.data()