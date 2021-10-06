import sqlite3
from sys import stderr
import argparse


parser = argparse.ArgumentParser(description="Food Blog")
parser.add_argument("database")
parser.add_argument("-i", "--ingredients", help="List of ingredients separated by commas.")
parser.add_argument("-m", "--meals", help="List of meals separated by commas.")
args = parser.parse_args()
database = args.database


class Recipe:

    def __init__(self, name, description=None, meals=None):
        self.id = None
        self.name = name
        self.description = description
        self.meals = meals or []


def check_if_empty(table_name):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()
        query = f"""SELECT COUNT(1)
                   FROM {table_name}"""
        result = cur.execute(query).fetchone()
        con.commit()
        cur.close()
        return not bool(result[0])
    except sqlite3.Error as err:
        stderr.write(f"Error while checking if empty a table:\n{err}\n")
    finally:
        if con:
            con.close()


def create_table(create_sql):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()
        cur.execute(create_sql)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while creating a table:\n{err}\n")
    finally:
        if con:
            con.close()


def create_tables():
    create_meals_table_sql = """CREATE TABLE IF NOT EXISTS meals (
                                    meal_id INTEGER PRIMARY KEY,
                                    meal_name TEXT UNIQUE NOT NULL
                                );"""
    create_ingredients_table_sql = """CREATE TABLE IF NOT EXISTS ingredients (
                                        ingredient_id INTEGER PRIMARY KEY,
                                        ingredient_name TEXT UNIQUE NOT NULL
                                      );"""
    create_measures_table_sql = """CREATE TABLE IF NOT EXISTS measures (
                                        measure_id INTEGER PRIMARY KEY,
                                        measure_name TEXT UNIQUE
                                   );"""
    tables = [create_meals_table_sql, create_ingredients_table_sql, create_measures_table_sql]
    for tbl in tables:
        create_table(tbl)

    create_recipes_table_sql = """CREATE TABLE IF NOT EXISTS recipes (
                                            recipe_id INTEGER PRIMARY KEY,
                                            recipe_name TEXT NOT NULL,
                                            recipe_description TEXT
                                  );"""
    create_table(create_recipes_table_sql)

    create_table("PRAGMA foreign_keys = ON;")
    create_serve_table_sql = """CREATE TABLE IF NOT EXISTS serve (
                                        serve_id INTEGER PRIMARY KEY,
                                        recipe_id INTEGER NOT NULL,
                                        meal_id INTEGER NOT NULL,
                                        FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
                                        FOREIGN KEY (meal_id) REFERENCES meals(meal_id)
                                  );"""
    create_table(create_serve_table_sql)

    create_quantity_table_sql = """CREATE TABLE IF NOT EXISTS quantity (
                                            quantity_id INTEGER PRIMARY KEY,
                                            recipe_id INTEGER NOT NULL,
                                            quantity INTEGER NOT NULL,
                                            measure_id INTEGER NOT NULL,
                                            ingredient_id INTEGER NOT NULL,
                                            FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
                                            FOREIGN KEY (measure_id) REFERENCES measures(measure_id),
                                            FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id)
                                      );"""
    create_table(create_quantity_table_sql)


def add_meals(meals):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO meals
                   (meal_name)
                   VALUES (?);"""
        cur.executemany(query, meals)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting meals:\n{err}\n")
    finally:
        if con:
            con.close()


def add_ingredients(ingredients):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO ingredients
                   (ingredient_name)
                   VALUES (?);"""
        data = [(i,) for i in ingredients]
        cur.executemany(query, data)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting ingredients:\n{err}\n")
    finally:
        if con:
            con.close()


def add_recipe(recipe):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO recipes
                   (recipe_name, recipe_description)
                   VALUES (?, ?);"""
        recipe.id = cur.execute(query, (recipe.name, recipe.description)).lastrowid
        for meal in recipe.meals:
            query = """INSERT INTO serve
                       (recipe_id, meal_id)
                       VALUES (?, ?);"""
            cur.execute(query, (recipe.id, meal))
        con.commit()
        cur.close()
        return recipe.id
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting a recipe:\n{err}\n")
    finally:
        if con:
            con.close()


def add_serve(data):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO serve
                   (recipe_id, meal_id)
                   VALUES (?, ?);"""
        cur.execute(query, data)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting a serve:\n{err}\n")
    finally:
        if con:
            con.close()


def add_measures(measures):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO measures
                   (measure_name)
                   VALUES (?);"""
        data = [(i,) for i in measures]
        cur.executemany(query, data)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting measures:\n{err}\n")
    finally:
        if con:
            con.close()


def get_measure_id(measure_name):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = f"""SELECT measure_id
                   FROM measures
                   WHERE measure_name {f'LIKE "%{measure_name}%"' if measure_name else '= ""'}"""
        result = cur.execute(query).fetchall()
        if len(result) > 1:
            raise QuantityException("measure")
        cur.close()
        return result[0][0]
    except sqlite3.Error as err:
        stderr.write(f"Error while getting a measure id:\n{err}\n")
    finally:
        if con:
            con.close()


def get_ingredient_id(ingredient_name):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = f"""SELECT ingredient_id
                   FROM ingredients
                   WHERE ingredient_name LIKE '%{ingredient_name}%'"""
        result = cur.execute(query).fetchall()
        if len(result) > 1:
            raise QuantityException("ingredient")
        cur.close()
        if result:
            return result[0][0]
        return None
    except sqlite3.Error as err:
        stderr.write(f"Error while getting an ingredient id:\n{err}\n")
    finally:
        if con:
            con.close()


def get_meal_id(meal_name):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = f"""SELECT meal_id
                    FROM meals
                    WHERE meal_name = ?"""

        result = cur.execute(query, (meal_name,)).fetchone()
        cur.close()
        if result:
            return result[0]
        return None
    except sqlite3.Error as err:
        stderr.write(f"Error while getting an meal id:\n{err}\n")
    finally:
        if con:
            con.close()


class QuantityException(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"The {self.value} is not conclusive!"


def add_quantity(recipe_id, quantity, measure, ingredient):
    con = None
    measure_id = get_measure_id(measure)
    ingredient_id = get_ingredient_id(ingredient)
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        query = """INSERT INTO quantity
                   (recipe_id, quantity, measure_id, ingredient_id)
                   VALUES (?, ?, ?, ?);"""
        data = (recipe_id, quantity, measure_id, ingredient_id)
        cur.execute(query, data)
        con.commit()
        cur.close()
    except sqlite3.Error as err:
        stderr.write(f"Error while inserting a quantity:\n{err}\n")
    finally:
        if con:
            con.close()


def adding_recipes():
    while True:
        print("Pass the empty recipe name to exit.")
        name = input("Recipe name: ")
        if not name:
            break
        desc = input("Recipe description: ")
        print("1) breakfast  2) brunch  3) lunch  4) supper")
        meals = [int(num) for num in input("When the dish can be served: ").split()]
        recipe_id = add_recipe(Recipe(name, desc, meals))

        # add ingredients
        while True:
            user_input = input("Input quantity of ingredient <press enter to stop>: ").split()
            if not user_input:
                break
            if len(user_input) == 2:
                quantity, ingredient = user_input
                measure = ""
            else:
                quantity, measure, ingredient = user_input
            try:
                add_quantity(recipe_id, quantity, measure, ingredient)
            except QuantityException as err:
                print(err)


def find_dish_for_ingredients(ingredients, meals):
    con = None
    try:
        con = sqlite3.connect(database)
        cur = con.cursor()

        meal_ids = []
        for meal in meals:
            m_id = get_meal_id(meal)
            if m_id is None:
                return []
            meal_ids.append(m_id)

        query = """SELECT DISTINCT recipe_id
                   FROM quantity q
                   WHERE """
        for i, ingredient in enumerate(ingredients):
            i_id = get_ingredient_id(ingredient)
            if i_id is None:
                return []
            if i == 0:
                query += f"ingredient_id = {i_id}"
            else:
                query += f""" AND EXISTS (SELECT recipe_id
                                         FROM quantity
                                         WHERE recipe_id = q.recipe_id AND ingredient_id = {i_id})"""
        recipe_ingredient_ids = [result[0] for result in cur.execute(query).fetchall()]

        query = f"""SELECT recipe_name
                    FROM recipes
                    WHERE recipe_id IN (SELECT DISTINCT recipe_id
                                        FROM serve
                                        WHERE meal_id 
                                        {f'IN {tuple(meal_ids)}' if len(meal_ids) > 1 else f'= {meal_ids[0]}'})
                    AND recipe_id 
                    {f'IN {tuple(recipe_ingredient_ids)}' if len(recipe_ingredient_ids) > 1 else f'= {recipe_ingredient_ids[0]}'}
                    """
        result = cur.execute(query).fetchall()
        cur.close()
        return [r[0] for r in result]
    except sqlite3.Error as err:
        stderr.write(f"Error while getting a dish for ingredients:\n{err}\n")
    finally:
        if con:
            con.close()


def main():
    pass
    create_tables()
    if check_if_empty("meals"):
        add_meals([("breakfast",), ("brunch",), ("lunch",), ("supper",)])
        add_ingredients(["milk", "cacao", "strawberry", "blueberry", "blackberry", "sugar"])
        add_measures(["ml", "g", "l", "cup", "tbsp", "tsp", "dsp", ""])
    if args.ingredients and args.meals:
        ingredients = [i.strip() for i in args.ingredients.split(",")]
        meals = [i.strip() for i in args.meals.split(",")]
        dishes = find_dish_for_ingredients(ingredients, meals)
        if dishes:
            print(f"Recipes selected for you: {', '.join(sorted(dishes))}")
        else:
            print("There are no such recipes in the database.")
    else:
        adding_recipes()


if __name__ == "__main__":
    main()
