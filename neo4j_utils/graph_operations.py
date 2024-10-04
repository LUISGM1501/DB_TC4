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
                MATCH (u:User)-[r:INTERACTED {type: 'rating'}]->(p:Product)
                WITH p, AVG(r.value) AS avg_rating, COUNT(r) AS num_ratings
                WHERE num_ratings > 10
                ORDER BY avg_rating DESC
                LIMIT 10
                RETURN p.productID, avg_rating, num_ratings
            """)
            return result.data()

    def user_product_clusters(self):
        with self.driver.session() as session:
            result = session.run("""
                CALL gds.graph.project(
                  'purchaseGraph',
                  ['User', 'Product'],
                  {
                    PURCHASED: {
                      type: 'PURCHASED',
                      orientation: 'UNDIRECTED'
                    }
                  }
                )
                YIELD graphName;
                
                CALL gds.louvain.stream('purchaseGraph')
                YIELD nodeId, communityId
                WITH gds.util.asNode(nodeId) AS node, communityId
                WHERE node:User
                RETURN communityId, COLLECT(node.userID) AS users
                ORDER BY SIZE(users) DESC
                LIMIT 10
            """)
            return result.data()