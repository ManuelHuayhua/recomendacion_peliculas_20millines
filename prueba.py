import psycopg2
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def get_movie_ratings(connection, movie_id):
    query = f"SELECT userId, rating FROM ratings WHERE movieId = {movie_id}"
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

def calculate_user_similarity(user_ratings, other_user_ratings, all_movie_ids):
    user_ratings_dict = dict(user_ratings)
    other_user_ratings_dict = dict(other_user_ratings)

    user_vector = [user_ratings_dict.get(movie_id, 0) for movie_id in all_movie_ids]
    other_user_vector = [other_user_ratings_dict.get(movie_id, 0) for movie_id in all_movie_ids]

    user_vector = np.array(user_vector).reshape(1, -1)
    other_user_vector = np.array(other_user_vector).reshape(1, -1)

    similarity = cosine_similarity(user_vector, other_user_vector)[0, 0]
    return similarity

def recommend_movies(connection, user_id):
    # Obtener todas las películas y sus IDs
    all_movie_query = "SELECT DISTINCT movieId FROM ratings"
    with connection.cursor() as cursor:
        cursor.execute(all_movie_query)
        all_movie_ids = [row[0] for row in cursor.fetchall()]

    # Obtener las películas que el usuario ha valorado
    user_ratings_query = f"SELECT movieId, rating FROM ratings WHERE userId = {user_id}"
    
    with connection.cursor() as cursor:
        cursor.execute(user_ratings_query)
        user_ratings = dict(cursor.fetchall())

        # Calcular la similitud entre usuarios
        user_similarities = {}
        for other_user_id in range(1, 100):  # Suponiendo que hay 100 usuarios en tu sistema
            if other_user_id != user_id:
                other_ratings_query = f"SELECT movieId, rating FROM ratings WHERE userId = {other_user_id}"
                cursor.execute(other_ratings_query)
                other_user_ratings = dict(cursor.fetchall())

                similarity = calculate_user_similarity(user_ratings, other_user_ratings, all_movie_ids)
                user_similarities[other_user_id] = similarity

        # Obtener los vecinos más cercanos (mayor similitud)
        nearest_neighbors = sorted(user_similarities.items(), key=lambda x: x[1], reverse=True)[:5]

        # Obtener las películas valoradas por los vecinos más cercanos
        recommended_movies = set()
        for neighbor_id, _ in nearest_neighbors:
            neighbor_ratings_query = f"SELECT movieId, rating FROM ratings WHERE userId = {neighbor_id}"
            cursor.execute(neighbor_ratings_query)
            neighbor_ratings = cursor.fetchall()

            for movie_id, rating in neighbor_ratings:
                if movie_id not in user_ratings:
                    recommended_movies.add(movie_id)

    return recommended_movies

try:
    connection = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='password',
        database='peliculas'
    )

    print("Conexión exitosa")

    # Ejemplo de recomendación para el usuario con ID 8
    user_id = 8
    recommendations = recommend_movies(connection, user_id)

    print(f"Recomendaciones para el usuario {user_id}: {recommendations}")

except Exception as ex:
    print("Error:", ex)

finally:
    # Cerrar la conexión
    if connection:
        connection.close()
