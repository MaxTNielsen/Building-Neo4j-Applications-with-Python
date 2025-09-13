from api.data import popular, goodfellas
from api.exceptions.notfound import NotFoundException

class FavoriteDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver


    """
    This method should retrieve a list of movies that have an incoming :HAS_FAVORITE
    relationship from a User node with the supplied `userId`.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.

    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """
    # tag::all[]
    def all(self, user_id, sort = 'title', order = 'ASC', limit = 6, skip = 0):
        # Retrieve a list of movies favorited by the user
        with self.driver.session() as session:
            movies = session.execute_read(lambda tx: tx.run("""
                MATCH (u:User {{userId: $userId}})-[r:HAS_FAVORITE]->(m:Movie)
                RETURN m {{
                    .*,
                    favorite: true
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
            """.format(sort, order), userId=user_id, limit=limit, skip=skip).value("movie"))

             # If no rows are returnedm throw a NotFoundException
            if movies == None:
                raise NotFoundException()
            
            return movies
    # end::all[]


    """
    This method should create a `:HAS_FAVORITE` relationship between
    the User and Movie ID nodes provided.
   *
    If either the user or movie cannot be found, a `NotFoundError` should be thrown.
    """
    # tag::add[]
    def add(self, user_id, movie_id):
        # Define a new transaction function to create a HAS_FAVORITE relationship
        def add_to_favorites(tx, user_id, movie_id):
            return tx.run("""
                MATCH (u:User {userId: $userId})
                MATCH (m:Movie {tmdbId: $movieId})
                MERGE (u)-[r:HAS_FAVORITE]->(m)
                ON CREATE SET u.createdAt = datetime()
                RETURN m {
                    .*,
                    favorite: true
                } AS movie
            """, userId=user_id, movieId=movie_id).single()

        with self.driver.session() as session:
            record = session.execute_write(add_to_favorites, user_id=user_id, movie_id=movie_id)

             # If no rows are returnedm throw a NotFoundException
            if record == None:
                raise NotFoundException()
            
            return record.get("movie")
    # end::add[]

    """
    This method should remove the `:HAS_FAVORITE` relationship between
    the User and Movie ID nodes provided.

    If either the user, movie or the relationship between them cannot be found,
    a `NotFoundError` should be thrown.
    """
    # tag::remove[]
    def remove(self, user_id, movie_id):
                # Define a new transaction function to create a HAS_FAVORITE relationship
        def remove_favorite(tx, user_id, movie_id):
            return tx.run("""
                MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie {tmdbId: $movieId})
                DELETE r
                RETURN m {
                    .*,
                    favorite: false
                } AS movie
            """, userId=user_id, movieId=movie_id).single()

        with self.driver.session() as session:
            record = session.execute_write(remove_favorite, user_id=user_id, movie_id=movie_id)

             # If no rows are returnedm throw a NotFoundException
            if record == None:
                raise NotFoundException()
            
            return record.get("movie")
    # end::remove[]
