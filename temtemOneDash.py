# %%
from operator import attrgetter
from datetime import datetime, timedelta
import os
from io import StringIO
from io import BytesIO
import re
import json
import pandas as pd
import numpy as np
import boto3
import bcrypt
import xlsxwriter
from st_files_connection import FilesConnection
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import toml

# import gspread
# from oauth2client.service_account import ServiceAccountCredentials


# %%
# Fonction pour charger les secrets depuis le fichier secrets.toml
def load_secrets():
    """
    Charge les secrets depuis le fichier 'secrets.toml'.

    Returns:
        dict: Un dictionnaire contenant les secrets chargés.
    """
    # Obtenez le chemin complet vers le fichier secrets.toml
    secrets_file_path = os.path.join(".streamlit", "secrets.toml")

    # Chargez le fichier secrets.toml
    secrets = toml.load(secrets_file_path)

    return secrets


# Fonction pour charger les données depuis S3 en utilisant les secrets du fichier toml
@st.cache_data
def load_data_from_s3_with_toml(secrets, bucket_name, file_name):
    """
    Charge les données depuis un fichier stocké sur Amazon S3 en utilisant les informations de connexion fournies.

    Args:
        secrets (dict): Un dictionnaire contenant les informations d'identification AWS.
        bucket_name (str): Le nom du bucket S3.
        file_name (str): Le nom du fichier à charger depuis S3.

    Returns:
        pd.DataFrame: Un DataFrame Pandas contenant les données du fichier chargé depuis S3.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=secrets["s3_credentials"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=secrets["s3_credentials"]["AWS_SECRET_ACCESS_KEY"],
    )
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    object_content = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(object_content), delimiter=",", low_memory=False)


# Fonction pour charger les données depuis S3 en utilisant experimental_connection
def load_data_from_s3_with_connection(bucket_name, file_name):
    """
    Charge les données depuis un fichier stocké sur Amazon S3 en utilisant la connexion expérimentale Streamlit.

    Args:
        bucket_name (str): Le nom du bucket S3.
        file_name (str): Le nom du fichier à charger depuis S3.

    Returns:
        pd.DataFrame: Un DataFrame Pandas contenant les données du fichier chargé depuis S3.
    """
    conn = st.experimental_connection("s3", type=FilesConnection)
    return conn.read(
        f"{bucket_name}/{file_name}",
        input_format="csv",
        ttl=600,
        low_memory=False,
    )


# Fonction pour charger key_google.json depuis S3 en tant qu'objet JSON
def load_key_google_json_with_json_key(secrets, bucket_name, file_name):
    """
    Charge une clé JSON Google depuis un fichier stocké sur Amazon S3 en utilisant les informations de connexion S3.

    Args:
        secrets (dict): Un dictionnaire contenant les informations de connexion S3.
        bucket_name (str): Le nom du bucket S3.
        file_name (str): Le nom du fichier à charger depuis S3.

    Returns:
        dict: Un objet JSON représentant la clé Google chargée depuis le fichier.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=secrets["s3_credentials"]["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=secrets["s3_credentials"]["AWS_SECRET_ACCESS_KEY"],
    )
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    object_content = response["Body"].read().decode("utf-8")

    # Utilisez la bibliothèque json pour charger le contenu en tant qu'objet JSON
    return json.loads(object_content)


def load_key_google_json_with_connection(bucket_name, file_name):
    """
    Charge une clé JSON Google depuis un fichier stocké sur Amazon S3 en utilisant la connexion expérimentale Streamlit.

    Args:
        bucket_name (str): Le nom du bucket S3.
        file_name (str): Le nom du fichier à charger depuis S3.

    Returns:
        dict: Un objet JSON représentant la clé Google chargée depuis le fichier.
    """
    conn = st.experimental_connection("s3", type=FilesConnection)
    return conn.read(
        f"{bucket_name}/{file_name}",
        input_format="json",  # Modifier le format d'entrée en 'json'
        ttl=600,
    )


# Mode de fonctionnement (Codespaces ou production en ligne)
mode = "production"  # Vous pouvez définir ceci en fonction de votre environnement

# Nom du seau S3
bucket_name = "one-data-lake"

# Liste des noms de fichiers à télécharger depuis S3
file_names = [
    "csv_database/orders.csv",
    "csv_database/ltv_data.csv",
    # "csv_database/users_2023.csv",
    "csv_database/customer_geolocalisation.csv",
    "key_google_json/key_google.json",
]

# Dictionnaire pour stocker les DataFrames correspondants aux fichiers
dataframes = {}

# Charger les secrets
secrets = load_secrets()

# Charger key_google.json en tant qu'objet JSON
if mode == "production":
    key_google_json = load_key_google_json_with_connection(
        bucket_name, "key_google_json/key_google.json"
    )
else:
    key_google_json = load_key_google_json_with_json_key(
        secrets, bucket_name, "key_google_json/key_google.json"
    )

# Charger les données depuis S3 en fonction du mode
for file_name in file_names:
    if "key_google_json" not in file_name:
        df_name = file_name.rsplit("/", 1)[-1].split(".")[
            0
        ]  # Obtenir le nom du DataFrame
        if mode == "production":
            dataframes[df_name] = load_data_from_s3_with_connection(
                bucket_name, file_name
            )
        else:
            dataframes[df_name] = load_data_from_s3_with_toml(
                secrets, bucket_name, file_name
            )

# Créer un DataFrame à partir des données
orders = dataframes["orders"]
ltv_data = dataframes["ltv_data"]
# users = dataframes["users_2023"]
geoloc_wilaya = dataframes["customer_geolocalisation"]

# %%
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 0)

orders["order_id"] = orders["order_id"].astype(str)
orders["customer_id"] = orders["customer_id"].astype(str)
orders["date"] = pd.to_datetime(orders["date"])
orders = orders.rename(columns={"job_status": "Status"})
orders = orders[~orders["Status"].isin(["ABANDONED"])]
orders["customer_id"] = [
    re.sub(r"\.0$", "", customer_id) for customer_id in orders["customer_id"]
]
orders["businessCat"] = orders["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
)

orders = orders[
    ~orders["order_id"].isin(
        [
            "734138951872",
            "811738356736",
            "648042957760",
            "239046556928",
            "423486580736",
            "536463465088",
        ]
    )
]
orders = orders[
    ~orders["customer_id"].isin(
        [
            "2059318",
            "1506025442",
            "1694397201",
            "2830181885",
            "5620828389",
            "4064611739",
            "3385745613",
            "2281370",
            "64438759505",
            "569994573568",
            "1628682",
            "310179181696",
            "878446",
            "3643707",
            "2253354",
            "1771017743",
            "727840660224",
            "2280761953",
            "2864429",
            "1505970032",
            "1517116",
            "929482210496",
            "5884716233",
            "22781605568",
            "2794629",
            "47201675489",
            "6072524763",
            "2342577",
            "1440074",
            "3666483",
            "449701472960",
            "869120",
            "7304625963",
            "2214784702",
            "869883",
            "2851778338",
            "3000794",
            "1898245261",
            "9816298466",
            "7021529167",
            "3017838801",
            "5624710564",
            "1584024035",
            "2485567",
            "2763532338",
            "841024809600",
            "1739473",
            "2183725",
            "3788062",
            "23400912794",
            "150321448192",
            "461317394880",
            "2208215",
            "3669307840",
            "610335616576",
            "7478577450",
            "13153632574",
            "2815691755",
            "879984",
            "3312616",
            "548088380288",
            "3526036",
            "2367635120",
            "24957125457",
            "459557812544",
            "1290757210",
            "507345740736",
            "2558315057",
            "819751",
            "407181581440",
            "1412707541",
            "1419613392",
            "4068655",
            "303655560704",
            "2389210",
            "2765139",
            "504153462208",
            "2100305133",
            "653243920384",
            "1253878877",
            "43255929830",
        ]
    )
]
orders = orders.rename(columns={"Order Type": "Order_Type"})
orders.loc[(orders["customer_id"] == "73187559488.0"), "Order_Type"] = "EXTERNE"
order_payment_screen = orders[
    ["date", "businessCat", "order_id", "Status", "customer_email"]
].rename(columns={"customer_email": "email"})
orders_pmi = orders[orders["Order_Type"] == "EXTERNE"]

ltv_data["total_amount_eur"] = ltv_data["total_amount_dzd"] * ltv_data["EUR"]
ltv_data = ltv_data[
    ltv_data["businessCat"].isin(["Airtime", "Alimentation", "Shopping"])
]
ltv_data["order_id"] = ltv_data["order_id"].astype(str)
ltv_data["customer_id"] = ltv_data["customer_id"].astype(str)
ltv_data = ltv_data[~ltv_data["job_status"].isin(["ABANDONED"])]
ltv_data = ltv_data[
    ~ltv_data["order_id"].isin(
        [
            "734138951872",
            "811738356736",
            "648042957760",
            "239046556928",
            "423486580736",
            "536463465088",
        ]
    )
]
ltv_data = ltv_data[
    ~ltv_data["customer_id"].isin(
        [
            "2059318.0",
            "1506025442.0",
            "1694397201.0",
            "2830181885.0",
            "5620828389.0",
            "4064611739.0",
            "3385745613.0",
            "2281370.0",
            "64438759505.0",
            "569994573568.0",
            "1628682.0",
            "310179181696.0",
            "878446.0",
            "3643707.0",
            "2253354.0",
            "1771017743.0",
            "727840660224.0",
            "2280761953.0",
            "2864429.0",
            "1505970032.0",
            "1517116.0",
            "929482210496.0",
            "5884716233.0",
            "22781605568.0",
            "2794629.0",
            "47201675489.0",
            "6072524763.0",
            "2342577.0",
            "1440074.0",
            "3666483.0",
            "449701472960.0",
            "869120.0",
            "7304625963.0",
            "2214784702.0",
            "869883.0",
            "2851778338.0",
            "3000794.0",
            "1898245261.0",
            "9816298466.0",
            "7021529167.0",
            "3017838801.0",
            "5624710564.0",
            "1584024035.0",
            "2485567.0",
            "2763532338.0",
            "841024809600.0",
            "1739473.0",
            "2183725.0",
            "3788062.0",
            "23400912794.0",
            "150321448192.0",
            "461317394880.0",
            "2208215.0",
            "3669307840.0",
            "610335616576.0",
            "7478577450.0",
            "13153632574.0",
            "2815691755.0",
            "879984.0",
            "3312616.0",
            "548088380288.0",
            "3526036.0",
            "2367635120.0",
            "24957125457.0",
            "459557812544.0",
            "1290757210.0",
            "507345740736.0",
            "2558315057.0",
            "819751.0",
            "407181581440.0",
            "1412707541.0",
            "1419613392.0",
            "4068655.0",
            "303655560704.0",
            "2389210.0",
            "2765139.0",
            "504153462208.0",
            "2100305133.0",
            "653243920384.0",
            "1253878877.0",
            "43255929830.0",
        ]
    )
]

ltv_data = ltv_data[ltv_data["job_status"] == "COMPLETED"]

# users["customer_id"] = users["customer_id"].astype(str)
# users["customer_id"] = [
#     re.sub(r"\.0$", "", customer_id) for customer_id in users["customer_id"]
# ]
# users["tags"] = users["tags"].str.replace(r"\[|\]", "", regex=True)
# users["tags"] = users["tags"].str.replace(r"['\"]", "", regex=True)
# users = users[
#     ~users["customer_id"].isin(
#         [
#             "2059318",
#             "1506025442",
#             "1694397201",
#             "2830181885",
#             "5620828389",
#             "4064611739",
#             "3385745613",
#             "2281370",
#             "64438759505",
#             "569994573568",
#             "1628682",
#             "310179181696",
#             "878446",
#             "3643707",
#             "2253354",
#             "1771017743",
#             "727840660224",
#             "2280761953",
#             "2864429",
#             "1505970032",
#             "1517116",
#             "929482210496",
#             "5884716233",
#             "22781605568",
#             "2794629",
#             "47201675489",
#             "6072524763",
#             "2342577",
#             "1440074",
#             "3666483",
#             "449701472960",
#             "869120",
#             "7304625963",
#             "2214784702",
#             "869883",
#             "2851778338",
#             "3000794",
#             "1898245261",
#             "9816298466",
#             "7021529167",
#             "3017838801",
#             "5624710564",
#             "1584024035",
#             "2485567",
#             "2763532338",
#             "841024809600",
#             "1739473",
#             "2183725",
#             "3788062",
#             "23400912794",
#             "150321448192",
#             "461317394880",
#             "2208215",
#             "3669307840",
#             "610335616576",
#             "7478577450",
#             "13153632574",
#             "2815691755",
#             "879984",
#             "3312616",
#             "548088380288",
#             "3526036",
#             "2367635120",
#             "24957125457",
#             "459557812544",
#             "1290757210",
#             "507345740736",
#             "2558315057",
#             "819751",
#             "407181581440",
#             "1412707541",
#             "1419613392",
#             "4068655",
#             "303655560704",
#             "2389210",
#             "2765139",
#             "504153462208",
#             "2100305133",
#             "653243920384",
#             "1253878877",
#             "43255929830",
#         ]
#     )
# ]

# users["createdAt"] = pd.to_datetime(users["createdAt"])
# users["createdAt"] = users["createdAt"].dt.strftime("%Y-%m-%d")
# users["date"] = users["createdAt"]
# users["date"] = pd.to_datetime(users["date"])
# users = users.rename(columns={"Origine": "customer_origine"})
# users_info = users[["date", "email", "phone", "customer_origine", "customer_id"]]


# # key_google_json contient le contenu du fichier key_google.json que vous avez chargé depuis S3
# creds = ServiceAccountCredentials.from_json_keyfile_dict(key_google_json)

# # Autoriser l'accès à Google Sheets en utilisant les informations d'authentification
# gc = gspread.authorize(creds)

# # Ouvrez la feuille Google Sheets par son nom
# spreadsheet_name = "Téléchargement"  # Remplacez par le nom de votre feuille
# worksheet_name = "telechargement"  # Remplacez par le nom de l'onglet que vous souhaitez lire

# try:
#     spreadsheet = gc.open(spreadsheet_name)
#     worksheet = spreadsheet.worksheet(worksheet_name)
#     # Lire les données de la feuille Google Sheets en tant que DataFrame pandas
#     telechargement = pd.DataFrame(worksheet.get_all_records())


#     # st.title("Lecture de la feuille Google Sheets")

#     # # Affichez les données de la feuille Google Sheets en tant que tableau
#     # st.table(telechargement)
# except gspread.exceptions.SpreadsheetNotFound:
#     st.error(f"La feuille '{spreadsheet_name}' ou l'onglet '{worksheet_name}' n'a pas été trouvé.")

# # Liste des noms de feuilles
# sheet_names = ['First_open_date_2020-2021', 'First_open_date_2021-2022', 'First_open_date_2022-2023']

# # Initialiser une liste pour stocker les DataFrames
# dfs = []

# # Parcourir les feuilles et les stocker dans la liste
# for sheet_name in sheet_names:
#     spreadsheet = gc.open(sheet_name)

#     # Accès à l'onglet spécifique
#     worksheet = spreadsheet.worksheet('First_open_email')

#     # Lire les données de l'onglet
#     data = worksheet.get_all_values()

#     # Créer un DataFrame à partir des données
#     df = pd.DataFrame(data)

#     # Définir la première ligne comme en-tête
#     df.columns = df.iloc[0]
#     df = df[1:]

#     # Ajouter le DataFrame à la liste
#     dfs.append(df)

# # Concaténer tous les DataFrames dans un seul DataFrame
# first_open_data = pd.concat(dfs, ignore_index=True).drop_duplicates(subset='email', keep='first')
# new_signups_first_open_data = pd.merge(users_info, first_open_data, how="inner", on='email')

# # st.dataframe(new_signups_first_open_data)

# # Nom de la feuille et de l'onglet
# sheet_name = "Utilisateur qui sont arrivé au Payment_method_screen"
# worksheet_name = "Payment_method_screen"

# # Ouverture de la feuille et de l'onglet spécifique
# spreadsheet = gc.open(sheet_name)
# worksheet = spreadsheet.worksheet(worksheet_name)

# # Lire les données de l'onglet
# data_payment_screen = worksheet.get_all_values()

# # Créer un DataFrame à partir des données
# payment_screen = pd.DataFrame(data_payment_screen[1:], columns=data_payment_screen[0])  # Utiliser la première ligne comme en-tête
# payment_screen["Date de Payment method screen"] = pd.to_datetime(payment_screen["Date de Payment method screen"]).dt.strftime("%Y-%m-%d")
# payment_screen_users = pd.merge(users_info, payment_screen, how="inner", on='email')
# order_payment_screen["date"] = pd.to_datetime(order_payment_screen["date"])

# payment_screen_users_orders = pd.merge(payment_screen_users, order_payment_screen, on=['email', 'date'], how='inner')
# new_signups_checkout = payment_screen_users_orders[(payment_screen_users_orders['Status']!="COMPLETED")].drop_duplicates(subset='email', keep='last')

# st.dataframe(new_signups_checkout)

# %%
# Filtrer le DataFrame pour ne contenir que les colonnes nécessaires
orders["date"] = pd.to_datetime(orders["date"])

orders = orders[
    [
        "date",
        "Status",
        "customer_origine",
        "paymentType",
        "order_id",
        "Order_Type",
        "businessCat",
        "customer_id",
        "Occurence",
        "previous_order_date",
        "returning_customer",
        "customer_username",
        "customer_phone",
        "customer_email",
        "total_amount_dzd",
    ]
]

orders = orders[orders["businessCat"].notnull()]

# %%
# Créez une base de données utilisateur
# Accédez aux informations de l'utilisateur depuis les secrets
user1_username = st.secrets["st_utilisateurs_1"]["st_username"]
user1_password = st.secrets["st_utilisateurs_1"]["st_password"]

user2_username = st.secrets["st_utilisateurs_2"]["st_username"]
user2_password = st.secrets["st_utilisateurs_2"]["st_password"]


# Fonction de connexion
def login(user_db):
    """
    Gère le processus de connexion en demandant à l'utilisateur de saisir un nom d'utilisateur et un mot de passe.

    Args:
        user_db (dict): Un dictionnaire contenant les informations des utilisateurs, y compris les mots de passe hachés.

    Returns:
        bool: True si la connexion est réussie, False sinon.
    """
    st.title("Connexion")
    username = st.text_input("Nom d'utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
        if username in user_db:
            hashed_password = user_db[username]["mot_de_passe"]
            if bcrypt.checkpw(password.encode(), hashed_password):
                st.success("Connexion réussie !")
                return True
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect.")
        else:
            st.error("Nom d'utilisateur non trouvé.")

    return False


# Créez un dictionnaire user_db avec les informations d'utilisateur hachées
user_db = {
    user1_username: {
        "mot_de_passe": bcrypt.hashpw(user1_password.encode(), bcrypt.gensalt())
    },
    user2_username: {
        "mot_de_passe": bcrypt.hashpw(user2_password.encode(), bcrypt.gensalt())
    },
}


def verify_credentials(username, password):
    """
    Vérifie les informations d'identification de l'utilisateur.

    Args:
        username (str): Le nom d'utilisateur à vérifier.
        password (str): Le mot de passe à vérifier.

    Returns:
        bool: True si les informations d'identification sont valides, False sinon.
    Raises:
            KeyError: Si le nom d'utilisateur n'est pas trouvé dans la base de données.
    """
    if username in user_db:
        hashed_password = user_db[username]["mot_de_passe"]
        return bcrypt.checkpw(password.encode(), hashed_password)
    return False


# Fonction pour appliquer les filtres
@st.cache_data
def apply_filters(df, customer_origine, business_cat, start_date, end_date):
    """
    Applique des filtres au DataFrame en fonction des critères spécifiés.

    Args:
        df (pd.DataFrame): Le DataFrame d'origine.
        customer_origine (str): La valeur de la colonne "customer_origine" à filtrer.
        business_cat (str): La valeur de la colonne "businessCat" à filtrer.
        start_date (str): La date de début pour la plage de dates à filtrer.
        end_date (str): La date de fin pour la plage de dates à filtrer.

    Returns:
        pd.DataFrame: Un nouveau DataFrame contenant les données filtrées.

    Raises:
        ValueError: Si les colonnes spécifiées pour le filtrage n'existent pas dans le DataFrame.
    """
    filtered_data = df.copy()

    # if status != "Tous":
    #     filtered_data = filtered_data[filtered_data["Status"] == status]

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if business_cat != "Toutes":
        filtered_data = filtered_data[filtered_data["businessCat"] == business_cat]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def apply_filters_ltv(df, customer_origine, business_cat, start_date, end_date):
    """
    Applique des filtres au DataFrame pour le calcul de la valeur à vie (LTV).

    Args:
        df (pd.DataFrame): Le DataFrame d'origine.
        customer_origine (str): La valeur de la colonne "customer_origine" à filtrer.
        business_cat (str): La valeur de la colonne "businessCat" à filtrer.
        start_date (str): La date de début pour la plage de dates à filtrer.
        end_date (str): La date de fin pour la plage de dates à filtrer.

    Returns:
        pd.DataFrame: Un nouveau DataFrame contenant les données filtrées.

    Notes:
        - Cette fonction est généralement utilisée pour filtrer les données avant de calculer le LTV.
    """
    filtered_data = df.copy()

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if business_cat != "Toutes":
        filtered_data = filtered_data[filtered_data["businessCat"] == business_cat]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def apply_filters_summary(df, customer_origine, start_date, end_date):
    """
    Applique des filtres au DataFrame pour résumer les données.

    Args:
        df (pd.DataFrame): Le DataFrame d'origine.
        customer_origine (str): La valeur de la colonne "customer_origine" à filtrer.
        start_date (str): La date de début pour la plage de dates à filtrer.
        end_date (str): La date de fin pour la plage de dates à filtrer.

    Returns:
        pd.DataFrame: Un nouveau DataFrame contenant les données filtrées.

    Notes:
        - Cette fonction est généralement utilisée pour filtrer les données avant de créer un résumé.
    """
    filtered_data = df.copy()

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def apply_filters_users(df, customer_origine, customer_country, start_date, end_date):
    """
    Applique des filtres au DataFrame pour résumer les données des utilisateurs.

    Args:
        df (pd.DataFrame): Le DataFrame d'origine.
        customer_origine (str): La valeur de la colonne "customer_origine" à filtrer.
        customer_country (str): La valeur de la colonne "customer_country" à filtrer.
        start_date (str): La date de début pour la plage de dates à filtrer.
        end_date (str): La date de fin pour la plage de dates à filtrer.

    Returns:
        pd.DataFrame: Un nouveau DataFrame contenant les données filtrées.

    Notes:
        - Cette fonction est généralement utilisée pour filtrer les données avant de créer un résumé des utilisateurs.
    """
    filtered_data = df.copy()

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if customer_country != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_country"] == customer_country
        ]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


# Créer une application Streamlit
def main():
    """
    Fonction principale pour exécuter l'application.

    Cette fonction contient le code principal de l'application, y compris la création des pages,
    le traitement des données, et la génération des graphiques et des visualisations.

    Returns:
    None
    """
    st.title("Tableau de Bord TemtemOne")
    # Ajout d'un lien vers une autre application
    st.markdown(
        "[Aller vers l'application Acquisition & Retargeting](https://temtemone-dashboard-factorydigitale-marketing.streamlit.app)"
    )

    # # Zone de connexion
    # if "logged_in" not in st.session_state:
    #     st.session_state.logged_in = False

    # if not st.session_state.logged_in:
    #     st.subheader("Connexion Requise")
    #     username = st.text_input("Nom d'utilisateur")
    #     password = st.text_input("Mot de passe", type="password")

    #     if st.button("Se connecter"):
    #         if verify_credentials(username, password):
    #             st.session_state.logged_in = True
    #         else:
    #             st.error("Nom d'utilisateur ou mot de passe incorrect.")
    #     return

    # Créer un menu de navigation
    selected_page = st.sidebar.selectbox(
        "Sélectionnez un Tableau de Bord",
        [
            "Retention",
            "Lifetime Value (LTV)",
            "Concentration des clients par commune, Algérie",
        ],
    )

    ####################################################################################   RETENTION PAGES   #####################################################################

    if selected_page == "Retention":
        # Contenu de la page "Tableau de Bord de Retention"
        st.header("Retention")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début",
            (datetime.now() - timedelta(days=365)).replace(month=1, day=1).date(),
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # # Filtres
        # status_options = ["Tous"] + list(orders["Status"].unique())
        # status = st.sidebar.selectbox("Statut", status_options)

        customer_origine_options = ["Tous"] + list(orders["customer_origine"].unique())
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        business_cat_options = ["Toutes"] + list(orders["businessCat"].unique())
        business_cat = st.sidebar.selectbox("Business catégorie", business_cat_options)

        # Appliquer les filtres
        filtered_data = apply_filters(
            orders,
            customer_origine,
            business_cat,
            start_date,
            end_date,
        )

        filtered_data = filtered_data[filtered_data["Status"] == "COMPLETED"]

        # Afficher les données filtrées
        show_filtered_data = st.sidebar.checkbox("Afficher les données")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=True):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_filtered_data:
            st.subheader("Data Orders")
            st.dataframe(filtered_data)

            # Bouton pour télécharger le DataFrame au format Excel
            filtered_data_xlsx = to_excel(filtered_data, include_index=False)
            st.download_button(
                "Télécharger les Orders en Excel (.xlsx)",
                filtered_data_xlsx,
                f"Orders - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

        # Calculer et afficher l'analyse de cohorte
        st.subheader("Analyse de Cohorte")
        filtered_data.dropna(subset=["customer_id"], inplace=True)
        # filtered_data["date"] = pd.to_datetime(filtered_data["date"])

        filtered_data["order_period"] = filtered_data["date"].dt.to_period("M")
        filtered_data["cohort"] = (
            filtered_data.groupby("customer_id")["date"]
            .transform("min")
            .dt.to_period("M")
        )

        filtered_data_cohort = (
            filtered_data.groupby(["cohort", "order_period"])
            .agg(n_customers=("customer_id", "nunique"))
            .reset_index(drop=False)
        )

        filtered_data_cohort["period_number"] = (
            filtered_data_cohort["order_period"] - filtered_data_cohort["cohort"]
        ).apply(attrgetter("n"))

        cohort_pivot = filtered_data_cohort.pivot_table(
            index="cohort", columns="period_number", values="n_customers"
        )

        # Calculer les clients qui ont abandonné (churn) pour chaque cohort
        churned_customers = cohort_pivot.copy()
        churned_customers.iloc[:, 1:] = (
            cohort_pivot.iloc[:, 1:].values - cohort_pivot.iloc[:, :-1].values
        )
        churned_customers.columns = [
            f"Churn_{col}" for col in churned_customers.columns
        ]

        retention = cohort_pivot.divide(cohort_pivot.iloc[:, 0], axis=0)

        # Créez la heatmap de la matrice de Retention analysis en pourcentage
        retention.index = retention.index.strftime("%Y-%m")
        retention.columns = retention.columns.astype(str)

        # Créez une fonction pour formater les valeurs
        def format_value(x):
            if not pd.isna(x):
                return f"{x:.2f}"
            else:
                return ""

        # Appliquez la fonction pour formater les valeurs dans le DataFrame
        heatmap_data = retention * 100
        heatmap_data = heatmap_data.applymap(format_value)

        # Créez une liste des étiquettes d'axe X (period_number) et d'axe Y (cohort)
        x_labels = heatmap_data.columns.tolist()  # Liste des périodes (0, 1, 2, ...)
        y_labels = (
            heatmap_data.index.tolist()
        )  # Liste des cohortes (2023-01, 2023-02, ...)

        # Créez un graphique en utilisant px.imshow avec les étiquettes X et Y spécifiées
        fig_retention = px.imshow(heatmap_data, x=x_labels, y=y_labels)

        # Personnalisez le texte à afficher pour chaque point de données (gardez deux chiffres après la virgule)
        custom_data = [
            [f"{value:.2f}%" if value is not None else "" for value in row]
            for row in (retention * 100).values
        ]

        # Mettez à jour le texte personnalisé dans le graphique
        fig_retention.update_traces(
            customdata=custom_data, hovertemplate="%{customdata}<extra></extra>"
        )

        # Ajoutez les annotations dans les cases de la heatmap
        for i, y_label in enumerate(y_labels):
            for j, x_label in enumerate(x_labels):
                value = heatmap_data.iloc[i, j]
                if not pd.isna(value):
                    font_color = "black" if j == 0 else "white"

                    fig_retention.add_annotation(
                        text=value,
                        x=x_label,
                        y=y_label,
                        showarrow=False,
                        font=dict(color=font_color),
                    )

        # Créez la heatmap de la matrice du nombre de clients
        cohort_pivot.index = cohort_pivot.index.strftime("%Y-%m")
        cohort_pivot.columns = cohort_pivot.columns.astype(str)

        # Créez une liste des étiquettes d'axe X (period_number) et d'axe Y (cohort)
        x_labels = cohort_pivot.columns.tolist()  # Liste des périodes (0, 1, 2, ...)
        y_labels = (
            cohort_pivot.index.tolist()
        )  # Liste des cohortes (2023-01, 2023-02, ...)

        # Créez un graphique en utilisant px.imshow avec les étiquettes X et Y spécifiées
        fig_clients = px.imshow(cohort_pivot, x=x_labels, y=y_labels)

        # Ajoutez les annotations dans les cases de la heatmap
        for i, y_label in enumerate(y_labels):
            for j, x_label in enumerate(x_labels):
                value = cohort_pivot.iloc[i, j]
                if not pd.isna(value):
                    font_color = "black" if j == 0 else "white"

                    # Vérifier que la valeur est une chaîne de caractères
                    if not isinstance(value, str):
                        value = str(value)

                    fig_clients.add_annotation(
                        text=value,
                        x=x_label,
                        y=y_label,
                        showarrow=False,
                        font=dict(color=font_color),
                    )

        # Créez des onglets pour basculer entre les deux visualisations
        selected_visualization = st.radio(
            "Sélectionnez la visualisation", ["Retention Analysis", "Nombre de Clients"]
        )

        if selected_visualization == "Retention Analysis":
            # Affichez la heatmap de l'analyse de rétention
            st.plotly_chart(fig_retention)  # Utilisez le graphique fig_retention
        else:
            # Affichez la heatmap du nombre de clients
            st.plotly_chart(fig_clients)  # Utilisez le graphique fig_clients

        # Téléchargement de la  Rétention
        retention_analysis_xlsx = to_excel(retention, include_index=True)
        st.download_button(
            "Télécharger la Retention analysis en Excel (.xlsx)",
            retention_analysis_xlsx,
            f"Retention analysis - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Téléchargement de la data de Client cohort en excel
        cohort_pivot_xlsx = to_excel(cohort_pivot, include_index=True)
        st.download_button(
            "Télécharger Client cohort en Excel (.xlsx)",
            cohort_pivot_xlsx,
            f"Client cohort - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    ####################################################################################   LTV PAGES   #####################################################################

    # Créez une nouvelle page LTV
    elif selected_page == "Lifetime Value (LTV)":
        st.header("Lifetime Value (LTV)")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début",
            (datetime.now() - timedelta(days=365)).replace(month=1, day=1).date(),
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # Filtres

        customer_origine_options = ["Tous"] + list(
            ltv_data["customer_origine"].unique()
        )
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        business_cat_options = ["Toutes"] + list(ltv_data["businessCat"].unique())
        business_cat = st.sidebar.selectbox("Business catégorie", business_cat_options)

        # Appliquer les filtres
        filtered_data_ltv = apply_filters_ltv(
            ltv_data,
            customer_origine,
            business_cat,
            start_date,
            end_date,
        )

        # Grouper les commandes par 'customer_id' et calculer le nombre de commandes et le montant total dépensé pour chaque client sur les données filtrées
        ltv_df = filtered_data_ltv.groupby("customer_id").agg(
            {
                "order_id": "count",
                "total_amount_dzd": "sum",
                "total_amount_eur": "sum",
                "marge_dzd": "sum",
                "marge_eur": "sum",
                "date": ["min", "max"],
            }
        )
        ltv_df.columns = [
            "Nombre de commandes",
            "Chiffre d'affaire en dzd",
            "Chiffre d'affaire en €",
            "Marge DZD",
            "Marge EUR",
            "min_date",
            "max_date",
        ]

        ltv_df = ltv_df.reset_index()

        # Calculer la durée de vie de chaque client en mois sur les données filtrées
        ltv_df["Durée de vie d’un client (lifetime)"] = (
            ltv_df["max_date"] - ltv_df["min_date"]
        ).dt.days / 30

        # Supprimer les clients ayant une durée de vie nulle (uniquement une commande) sur les données filtrées
        ltv_df = ltv_df[ltv_df["Durée de vie d’un client (lifetime)"] > 0]

        # Diviser le montant total dépensé par le nombre de commandes pour obtenir la valeur moyenne des commandes sur les données filtrées
        ltv_df["Panier moyen en dzd"] = (
            ltv_df["Chiffre d'affaire en dzd"] / ltv_df["Nombre de commandes"]
        )

        ltv_df["Panier moyen en €"] = (
            ltv_df["Chiffre d'affaire en €"] / ltv_df["Nombre de commandes"]
        )

        ltv_df["Panier moyen (marge en dzd)"] = (
            ltv_df["Marge DZD"] / ltv_df["Nombre de commandes"]
        )

        ltv_df["Panier moyen (marge en €)"] = (
            ltv_df["Marge EUR"] / ltv_df["Nombre de commandes"]
        )

        # Diviser le nombre de commandes par la durée de vie de chaque client pour obtenir la fréquence d'achat sur les données filtrées
        ltv_df["Fréquence d’achat"] = (
            ltv_df["Nombre de commandes"]
            / ltv_df["Durée de vie d’un client (lifetime)"]
        )

        # Calculer la LTV en multipliant la fréquence d'achat par la valeur moyenne des commandes et en multipliant le résultat par la durée de vie du client en mois sur les données filtrées
        ltv_df["LTV (GMV en dzd)"] = (
            ltv_df["Fréquence d’achat"]
            * ltv_df["Panier moyen en dzd"]
            * ltv_df["Durée de vie d’un client (lifetime)"]
        )

        ltv_df["LTV (GMV en €)"] = (
            ltv_df["Fréquence d’achat"]
            * ltv_df["Panier moyen en €"]
            * ltv_df["Durée de vie d’un client (lifetime)"]
        )

        ltv_df["LTV (Marge en dzd)"] = (
            ltv_df["Fréquence d’achat"]
            * ltv_df["Panier moyen (marge en dzd)"]
            * ltv_df["Durée de vie d’un client (lifetime)"]
        )

        ltv_df["LTV (Marge en €)"] = (
            ltv_df["Fréquence d’achat"]
            * ltv_df["Panier moyen (marge en €)"]
            * ltv_df["Durée de vie d’un client (lifetime)"]
        )

        # Afficher les données filtrées
        show_ltv_df = st.sidebar.checkbox("Afficher les données")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=True):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_ltv_df:
            st.subheader("LTV Data")
            st.dataframe(ltv_df)

            # Bouton pour télécharger le DataFrame au format Excel
            ltv_df_xlsx = to_excel(ltv_df, include_index=False)
            st.download_button(
                "Télécharger les données de la LTV en Excel (.xlsx)",
                ltv_df_xlsx,
                f"LTV - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Appliquer les filtres
        filtered_data_ltv_summary = apply_filters_summary(
            ltv_data,
            customer_origine,
            start_date,
            end_date,
        )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

        # Liste unique de catégories d'entreprise
        business_cats = filtered_data_ltv_summary["businessCat"].unique()

        # Initialiser une liste pour stocker les résultats par Business Catégorie
        ltv_results = []

        # Parcourir chaque Business Catégorie et calculer la LTV
        for business_cat in business_cats:
            # Filtrer les données pour la Business Catégorie actuelle
            ltv_cat_df = filtered_data_ltv_summary[
                filtered_data_ltv_summary["businessCat"] == business_cat
            ]

            # Grouper les commandes par 'customer_id' et calculer le nombre de commandes et le montant total dépensé pour chaque client sur les données filtrées
            ltv_df_summary_cat = ltv_cat_df.groupby("customer_id").agg(
                {
                    "order_id": "count",
                    "total_amount_dzd": "sum",
                    "total_amount_eur": "sum",
                    "marge_dzd": "sum",
                    "marge_eur": "sum",
                    "date": ["min", "max"],
                }
            )

            ltv_df_summary_cat.columns = [
                "Nombre de commandes",
                "Chiffre d'affaire en dzd",
                "Chiffre d'affaire en €",
                "Marge DZD",
                "Marge EUR",
                "min_date",
                "max_date",
            ]

            ltv_df_summary_cat = ltv_df_summary_cat.reset_index()
            # Calculer la durée de vie de chaque client en mois sur les données filtrées
            ltv_df_summary_cat["Durée de vie d’un client (lifetime)"] = (
                ltv_df_summary_cat["max_date"] - ltv_df_summary_cat["min_date"]
            ).dt.days / 30

            # Supprimer les clients ayant une durée de vie nulle (uniquement une commande) sur les données filtrées
            ltv_df_summary_cat = ltv_df_summary_cat[
                ltv_df_summary_cat["Durée de vie d’un client (lifetime)"] > 0
            ]

            # Diviser le montant total dépensé par le nombre de commandes pour obtenir la valeur moyenne des commandes sur les données filtrées

            ltv_df_summary_cat["Panier moyen en dzd"] = (
                ltv_df_summary_cat["Chiffre d'affaire en dzd"]
                / ltv_df_summary_cat["Nombre de commandes"]
            )

            ltv_df_summary_cat["Panier moyen en €"] = (
                ltv_df_summary_cat["Chiffre d'affaire en €"]
                / ltv_df_summary_cat["Nombre de commandes"]
            )

            ltv_df_summary_cat["Panier moyen (marge en dzd)"] = (
                ltv_df_summary_cat["Marge DZD"]
                / ltv_df_summary_cat["Nombre de commandes"]
            )

            ltv_df_summary_cat["Panier moyen (marge en €)"] = (
                ltv_df_summary_cat["Marge EUR"]
                / ltv_df_summary_cat["Nombre de commandes"]
            )

            # Diviser le nombre de commandes par la durée de vie de chaque client pour obtenir la fréquence d'achat sur les données filtrées
            ltv_df_summary_cat["Fréquence d’achat"] = (
                ltv_df_summary_cat["Nombre de commandes"]
                / ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            # Calculer la LTV en multipliant la fréquence d'achat par la valeur moyenne des commandes et en multipliant le résultat par la durée de vie du client en mois sur les données filtrées

            ltv_df_summary_cat["LTV (GMV en dzd)"] = (
                ltv_df_summary_cat["Fréquence d’achat"]
                * ltv_df_summary_cat["Panier moyen en dzd"]
                * ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            ltv_df_summary_cat["LTV (GMV en €)"] = (
                ltv_df_summary_cat["Fréquence d’achat"]
                * ltv_df_summary_cat["Panier moyen en €"]
                * ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            ltv_df_summary_cat["LTV (Marge en dzd)"] = (
                ltv_df_summary_cat["Fréquence d’achat"]
                * ltv_df_summary_cat["Panier moyen (marge en dzd)"]
                * ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            ltv_df_summary_cat["LTV (Marge en €)"] = (
                ltv_df_summary_cat["Fréquence d’achat"]
                * ltv_df_summary_cat["Panier moyen (marge en €)"]
                * ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            # Ajouter une colonne "businessCat" pour indiquer la Business Catégorie
            ltv_df_summary_cat["businessCat"] = business_cat

            # Ajouter les résultats au tableau de résultats
            ltv_results.append(ltv_df_summary_cat)

        # Concaténer les résultats de toutes les catégories en un seul DataFrame
        ltv_summary_df = pd.concat(ltv_results, ignore_index=True)

        # Réorganiser les colonnes si nécessaire
        ltv_summary_df = ltv_summary_df[
            [
                "businessCat",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
                "LTV (GMV en dzd)",
                "LTV (Marge en dzd)",
            ]
        ]

        # st.write(ltv_summary_df)

        # Calculer la moyenne de LTV par Business Catégorie
        ltv_avg_by_cat = (
            ltv_summary_df.groupby("businessCat")[
                [
                    "LTV (GMV en €)",
                    "LTV (Marge en €)",
                    "LTV (GMV en dzd)",
                    "LTV (Marge en dzd)",
                ]
            ]
            .mean()
            .reset_index()
        )

        # Arrondir les colonnes "LTV (GMV en DZD)" et "LTV (Marge en DZD)" à zéro décimal
        ltv_avg_by_cat[
            [
                "LTV (GMV en dzd)",
                "LTV (Marge en dzd)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ] = ltv_avg_by_cat[
            [
                "LTV (GMV en dzd)",
                "LTV (Marge en dzd)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ].round(
            0
        )

        # Convertir les colonnes en types de données entiers
        ltv_avg_by_cat[
            [
                "LTV (GMV en dzd)",
                "LTV (Marge en dzd)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ] = ltv_avg_by_cat[
            [
                "LTV (GMV en dzd)",
                "LTV (Marge en dzd)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ].astype(
            int
        )

        # Renommer la colonne "businessCat" en "Business Catégorie"
        ltv_avg_by_cat.rename(
            columns={"businessCat": "Business Catégorie"}, inplace=True
        )

        # Calculer le pourcentage entre "LTV (Marge en dzd)" et "LTV (GMV en dzd)" pour chaque catégorie d'entreprise
        ltv_avg_by_cat["Pourcentage Marge vs GMV"] = (
            ltv_avg_by_cat["LTV (Marge en dzd)"] / ltv_avg_by_cat["LTV (GMV en dzd)"]
        ) * 100

        # Formater la colonne "Business Catégorie" avec le pourcentage
        ltv_avg_by_cat["Business Catégorie"] = (
            ltv_avg_by_cat["Business Catégorie"]
            + " ("
            + ltv_avg_by_cat["Pourcentage Marge vs GMV"].round(2).astype(str)
            + "%)"
        )

        ltv_avg_by_cat = ltv_avg_by_cat.drop(columns=["Pourcentage Marge vs GMV"])

        ltv_avg_by_cat["LTV (GMV en dzd)"] = ltv_avg_by_cat["LTV (GMV en dzd)"].apply(
            lambda x: "{:,.0f}".format(x).replace(",", ".")
        )
        ltv_avg_by_cat["LTV (Marge en dzd)"] = ltv_avg_by_cat[
            "LTV (Marge en dzd)"
        ].apply(lambda x: "{:,.0f}".format(x).replace(",", "."))

        # Afficher le tableau de la LTV
        st.subheader("Moyenne de LTV par Business Catégorie (GMV et Marge de la GMV) :")
        st.dataframe(ltv_avg_by_cat)

        # Téléchargement de la LTV
        ltv_avg_by_cat_xlsx = to_excel(ltv_avg_by_cat, include_index=False)
        st.download_button(
            "Télécharger LTV par Business Catégorie (.xlsx)",
            ltv_avg_by_cat_xlsx,
            f"LTV par Business Catégorie - ORIGINE : {customer_origine}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Créer une fonction pour générer le graphique
        def generate_ltv_graph(df, devise):
            fig = go.Figure()

            if devise == "€":
                columns = ["LTV (GMV en €)", "LTV (Marge en €)"]
                names = ["LTV (GMV en €)", "LTV (Marge en €)"]
            else:
                columns = ["LTV (GMV en dzd)", "LTV (Marge en dzd)"]
                names = ["LTV (GMV en dzd)", "LTV (Marge en dzd)"]

            for col, name in zip(columns, names):
                fig.add_trace(
                    go.Bar(
                        x=df["Business Catégorie"],
                        y=df[col],
                        name=name,
                    )
                )

            fig.update_layout(
                barmode="group",
                title="LTV par Business Catégorie",
                xaxis_title="Business Catégorie",
                yaxis_title="LTV",
                legend_title="Devise",
            )

            return fig

        # Afficher le graphique dans Streamlit
        st.subheader("LTV par Business Catégorie")

        # Sélection de la devise
        selected_devise = st.selectbox("Sélectionnez la devise :", ["€", "DZD"])

        devise = ""

        if selected_devise != devise:
            devise = selected_devise
            st.plotly_chart(generate_ltv_graph(ltv_avg_by_cat, devise))

    ####################################################################################   USERS PAGES   #####################################################################

    # # Créez une nouvelle page Users
    # elif selected_page == "Users":
    #     st.header("Users 2023")

    #     # Sidebar pour les filtres
    #     st.sidebar.title("Filtres")

    #     # Sélection manuelle de la date de début
#     start_date = st.sidebar.date_input(
#     "Date de début", (datetime.now() - timedelta(days=365)).replace(month=1, day=1).date()
# )
    #     end_date = st.sidebar.date_input(
    #         "Date de fin", pd.to_datetime(orders["date"].max()).date()
    #     )

    #     # Filtres
    #     customer_origine_options = ["Tous"] + list(users["customer_origine"].unique())
    #     customer_origine = st.sidebar.selectbox(
    #         "Customer Origine (diaspora or Local)", customer_origine_options
    #     )

    #     customer_country_options = ["Tous"] + list(users["customer_country"].unique())
    #     customer_country = st.sidebar.selectbox(
    #         "Customer Country", customer_country_options
    #     )

    #     # Appliquer les filtres
    #     filtered_new_signups = apply_filters_users(
    #         users,
    #         customer_origine,
    #         customer_country,
    #         # accountTypes,
    #         # tags,
    #         start_date,
    #         end_date,
    #     )

    #     filtered_data_download = apply_filters_summary(
    #         telechargement,
    #         customer_origine,
    #         start_date,
    #         end_date,
    #     )

    #     filtered_new_signups_first_open = apply_filters_summary(
    #         new_signups_first_open_data,
    #         customer_origine,
    #         start_date,
    #         end_date,
    #     )

    #     filtered_new_signups_first_open = filtered_new_signups_first_open.drop_duplicates(subset="email")

    #     # st.dataframe(filtered_new_signups_first_open)

    #     filtered_new_signups_checkout_data = apply_filters_summary(
    #         new_signups_checkout,
    #         customer_origine,
    #         start_date,
    #         end_date,
    #     )
    #     filtered_new_signups_checkout_data = filtered_new_signups_checkout_data.drop_duplicates(subset="email")
    #     # st.dataframe(filtered_new_signups_checkout_data)

    # # Afficher les données des Users
    # show_filtered_new_signups = st.sidebar.checkbox("Afficher les données des Users")

    # # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
    # def to_excel(df, include_index=False):
    #     output = BytesIO()
    #     with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    #         df.to_excel(writer, index=include_index, sheet_name="Sheet1")
    #         workbook = writer.book
    #         worksheet = writer.sheets["Sheet1"]
    #         format = workbook.add_format({"num_format": "0.00"})
    #         worksheet.set_column("A:A", None, format)
    #     processed_data = output.getvalue()
    #     return processed_data

    # if show_filtered_new_signups:
    #     st.subheader("Data Users")
    #     st.dataframe(filtered_new_signups)

    #     # Bouton pour télécharger le DataFrame au format Excel
    #     filtered_new_signups_xlsx = to_excel(
    #         filtered_new_signups, include_index=False
    #     )
    #     st.download_button(
    #         "Télécharger les Users en Excel (.xlsx)",
    #         filtered_new_signups_xlsx,
    #         f"USERS - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    ###################

    # # Sélectionnez les nouveaux inscrits en fonction des filtres déjà appliqués
    # new_signups = filtered_new_signups
    # new_signups = new_signups[
    #     [
    #         "date",
    #         "customer_id",
    #         "lastName",
    #         "firstName",
    #         "phone",
    #         "email",
    #         "customer_country",
    #         "customer_origine",
    #     ]
    # ]

    # orders_users = orders.copy()
    # orders_users = orders_users[orders_users["date"] >= "2023-01-01"]
    # orders_users = orders_users[
    #     [
    #         "date",
    #         "customer_id",
    #         "order_id",
    #         "Status",
    #         "customer_origine",
    #         "businessCat",
    #         "total_amount_dzd"
    #     ]

    # ].rename(columns={"customer_origine": "customer_origine_orders"})

    # new_signups_copy = new_signups.copy()
    # new_signups_copy = new_signups_copy.rename(
    #     columns={"date": "registration_date"}
    # )

    # new_signups_ordered = pd.merge(
    #     orders_users, new_signups_copy, how="inner", on="customer_id"
    # )

    # new_signups_ordered['New_status'] = new_signups_ordered['Status']
    # new_signups_ordered = new_signups_ordered.copy()
    # new_signups_ordered[
    #     "New_status"
    # ] = new_signups_ordered["New_status"].map(
    #     lambda x: "NOT COMPLETED" if x != "COMPLETED" else x
    # )

    # # Sélection de la granularité de la période
    # granularity = st.radio(
    #     "Sélectionnez la période",
    #     ["Mois", "Semaine", "Jour"],
    #     key="granularity_users",
    # )

    # # Créez une nouvelle colonne "period" pour définir les cohortes en fonction de la granularité sélectionnée
    # if granularity == "Jour":
    #     new_signups["period"] = new_signups["date"]
    #     period_duration = pd.Timedelta(days=1)  # Une journée
    # elif granularity == "Semaine":
    #     new_signups["period"] = new_signups["date"] - pd.to_timedelta(
    #         (new_signups["date"].dt.dayofweek + 1) % 7, unit="D"
    #     )
    #     period_duration = pd.Timedelta(days=7)  # Une semaine
    # else:
    #     new_signups["period"] = new_signups["date"].dt.strftime(
    #         "%Y-%m"
    #     )  # Convertir en format "YYYY-MM"
    #     period_duration = pd.Timedelta(days=31)  # Un mois

    # # Agrégez les données par période (jour, semaine ou mois) et comptez le nombre total d'inscriptions pour le mois
    # if period_duration:
    #     new_signups_count = (
    #         new_signups.groupby("period").size().reset_index(name="count")
    #     )
    # else:
    #     new_signups_count = (
    #         new_signups.groupby("period")
    #         .size()
    #         .reset_index(name="count")
    #         .groupby("period")
    #         .sum()
    #         .reset_index()
    #     )

    # # Assurez-vous que la dernière période se termine exactement à la fin de la période sélectionnée
    # if len(new_signups_count) > 0 and period_duration:
    #     first_period_start = new_signups_count["period"].min()
    #     last_period_end = first_period_start + period_duration
    #     new_signups_count.loc[0, "period"] = last_period_end

    # # Créez un graphique montrant le nombre de nouveaux inscrits par période
    # if period_duration:
    #     period_label = f"{granularity}"
    # else:
    #     period_label = "Mois"

    # fig_nmb_usr = px.bar(
    #     new_signups_count,
    #     x="period",
    #     y="count",
    #     title=f"Nombre de Nouveaux Inscrits par {period_label}",
    #     labels={"period": period_label, "count": "Nombre de Nouveaux Inscrits"},
    # ).update_xaxes(categoryorder="total ascending")

    # # Créez une liste de toutes les catégories de business
    # all_categories = ["Toutes les catégories"] + list(new_signups_ordered["businessCat"].unique())

    # selected_business_cat = st.selectbox(
    #     "Sélectionnez la catégorie de business",
    #     all_categories
    # )

    # # Dupliquez new_signups_ordered pour créer new_signups_ordered_copy
    # new_signups_ordered_copy = new_signups_ordered.copy().drop_duplicates(subset="customer_id")

    # if selected_business_cat == "Toutes les catégories":
    #     filtered_new_signups_ordered = new_signups_ordered_copy
    #     filtered_new_signups_checkout = filtered_new_signups_checkout_data
    # else:
    #     filtered_new_signups_ordered = new_signups_ordered_copy[new_signups_ordered_copy["businessCat"] == selected_business_cat]
    #     filtered_new_signups_checkout = filtered_new_signups_checkout_data[filtered_new_signups_checkout_data["businessCat"] == selected_business_cat]

    # filtered_new_signups_ordered_customer = filtered_new_signups_ordered['customer_id']
    # filtered_new_signups_not_ordered = new_signups[~new_signups['customer_id'].isin(filtered_new_signups_ordered_customer)].drop_duplicates(subset="customer_id")
    # filtered_new_signups_completed = filtered_new_signups_ordered[filtered_new_signups_ordered["New_status"] == "COMPLETED"].drop_duplicates(subset="customer_id")
    # filtered_new_signups_not_completed_customer = filtered_new_signups_completed['customer_id']
    # filtered_new_signups_not_completed = filtered_new_signups_ordered[~filtered_new_signups_ordered['customer_id'].isin(filtered_new_signups_not_completed_customer)].drop_duplicates(subset="customer_id")

    # # Calculez les mesures directement sur les données filtrées
    # total_filtered_downloads = filtered_data_download['Téléchargement'].sum()
    # total_filtered_new_signups = len(new_signups.drop_duplicates(subset="customer_id"))
    # total_filtered_new_signups_completed = len(filtered_new_signups_completed)
    # total_filtered_new_signups_ordered = len(filtered_new_signups_ordered)
    # total_filtered_new_signups_not_completed = len(filtered_new_signups_not_completed)
    # total_filtered_new_signups_not_ordered = len(filtered_new_signups_not_ordered)
    # total_filtered_new_signups_first_open = len(filtered_new_signups_first_open)
    # total_filtered_new_signups_checkout = len(filtered_new_signups_checkout)

    # # Créez un DataFrame avec les mesures calculées
    # filtered_stats_data = pd.DataFrame({
    #     'Mesure': ["Nombre de Téléchargement",
    #                "Nombre de Nouveaux Inscrit",
    #                "Nombre de Nouveaux Inscrit qui n'ont jamais effectué une commande",
    #                "Nombre de Nouveaux Inscrit qui ont overt la première fois l'app",
    #                "Nombre de Nouveaux Inscrit qui ont effectué au moins une commande",
    #                "Nombre de Nouveaux Inscrit qui ont effectué au moins un achat",
    #                "Nombre de Nouveaux Inscrit qui n'ont n'ont jamais effectué un achat",
    #                "Nombre de Nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté"],

    #     'Valeur': [total_filtered_downloads,
    #                total_filtered_new_signups,
    #                total_filtered_new_signups_not_ordered ,
    #                total_filtered_new_signups_first_open,
    #                total_filtered_new_signups_ordered,
    #                total_filtered_new_signups_completed,
    #                total_filtered_new_signups_not_completed,
    #                total_filtered_new_signups_checkout]
    # })

    # # Utilisez les données calculées pour créer le graphique
    # fig_filtered_stat = go.Figure(go.Bar(
    #     x=filtered_stats_data['Valeur'],
    #     y=filtered_stats_data['Mesure'],
    #     orientation='h',
    #     marker=dict(color=['blue', 'green', 'red', 'purple', 'orange', 'yellow', 'brown', 'pink']),
    #     text=filtered_stats_data['Valeur'],
    # ))

    # # Personnalisez le graphique
    # fig_filtered_stat.update_traces(texttemplate='%{text}', textposition='outside')

    # # Personnalisez la mise en page
    # fig_filtered_stat.update_layout(
    #     title=f'Statistiques des Nouveaux Inscrits - {selected_business_cat}',
    #     xaxis_title='Valeur',
    #     yaxis_title='Mesure'
    # )

    # # Créez des onglets pour basculer entre les deux visualisations
    # selected_visualization = st.radio(
    #     "Sélectionnez la visualisation",
    #     ["Nombre de Nouveaux Inscrits", "Statistiques des Nouveaux Inscrits"],
    # )

    # if selected_visualization == "Nombre de Nouveaux Inscrits":
    #     # Affichez la heatmap de l'analyse de rétention
    #     st.plotly_chart(fig_nmb_usr)  # Utilisez le graphique fig_nmb_usr
    # else:
    #     # Affichez la heatmap du nombre de clients
    #     st.plotly_chart(fig_filtered_stat)  # Utilisez le graphique fig_filtered_stat

    # # Afficher et téléchargerles nouveaux inscrits dans le tableau de bord

    #  # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
    # def to_excel(df, include_index=False):
    #     output = BytesIO()
    #     with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    #         df.to_excel(writer, index=include_index, sheet_name="Sheet1")
    #         workbook = writer.book
    #         worksheet = writer.sheets["Sheet1"]
    #         format = workbook.add_format({"num_format": "0.00"})
    #         worksheet.set_column("A:A", None, format)
    #     processed_data = output.getvalue()
    #     return processed_data

    # show_new_signups = st.sidebar.checkbox(
    #     "Afficher la liste des nouveaux inscrits"
    # )

    # if show_new_signups:
    #     st.subheader("Nouveaux Inscrits")
    #     st.dataframe(new_signups)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_xlsx = to_excel(new_signups, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des Nouveaux Inscrits (.xlsx)",
    #         new_signups_xlsx,
    #         f"Nouveaux Inscrits - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # show_new_signups_not_ordered = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux Inscrits qui n'ont jamais effectué une commande", key="checkbox_new_signups_not_ordered"
    # )

    # if show_new_signups_not_ordered:
    #     st.subheader("Les données des nouveaux Inscrits qui n'ont jamais effectué une commande")
    #     st.dataframe(filtered_new_signups_not_ordered)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_not_ordered_xlsx = to_excel(filtered_new_signups_not_ordered, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux Inscrits qui n'ont jamais effectué une commande (.xlsx)",
    #         new_signups_not_ordered_xlsx,
    #         f"Nouveaux Inscrits qui n'ont jamais effectué une commande - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # show_new_signups_first_open = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux inscrits avec la date du first open app", key="checkbox_new_signups_first_open"
    # )

    # if show_new_signups_first_open:
    #     st.subheader("Les données des nouveaux inscrits avec la date du first open app")
    #     st.dataframe(filtered_new_signups_first_open)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_first_open_xlsx = to_excel(filtered_new_signups_first_open, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux inscrits avec la date du first open app (.xlsx)",
    #         new_signups_first_open_xlsx,
    #         f"Nouveaux Inscrits avec la date du first open app - ORIGINE : {customer_origine}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # show_new_signups_ordered = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux Inscrits qui ont effectué au moins une commande", key="checkbox_new_signups_ordered"
    # )

    # if show_new_signups_ordered:
    #     st.subheader("Les données des nouveaux Inscrits qui ont effectué une commande")
    #     st.dataframe(filtered_new_signups_ordered)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_ordered_xlsx = to_excel(filtered_new_signups_ordered, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux Inscrits qui ont jamais effectué une commande (.xlsx)",
    #         new_signups_ordered_xlsx,
    #         f"Nouveaux Inscrits qui ont jamais effectué une commande - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # # st.write(len(filtered_new_signups_ordered))

    # show_new_signups_completed = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux Inscrits qui ont effectué au moins un achat", key="checkbox_new_signups_completed"
    # )

    # if show_new_signups_completed:
    #     st.subheader("Les données des nouveaux Inscrits qui ont effectué au moins un achat")
    #     st.dataframe(filtered_new_signups_completed)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_completed_xlsx = to_excel(filtered_new_signups_completed, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux Inscrits qui ont effectué au moins un achat (.xlsx)",
    #         new_signups_completed_xlsx,
    #         f"Nouveaux Inscrits qui ont effectué au moins un achat - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # show_new_signups_not_completed = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux Inscrits qui n'ont jamais effectué au moins un achat", key="checkbox_new_signups_not_completed"
    # )

    # if show_new_signups_not_completed:
    #     st.subheader("Les données des nouveaux Inscrits qui n'ont jamais effectué au moins un achat")
    #     st.dataframe(filtered_new_signups_not_completed)

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_not_completed_xlsx = to_excel(filtered_new_signups_not_completed, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux Inscrits qui n'ont jamais effectué au moins un achat (.xlsx)",
    #         new_signups_not_completed_xlsx,
    #         f"Nouveaux Inscrits qui n'ont jamais effectué au moins un achat - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # show_new_signups_checkout = st.sidebar.checkbox(
    #     "Afficher les données des nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté", key="checkbox_new_signups_checkout"
    # )

    # if show_new_signups_checkout:
    #     st.subheader("Les données des nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté")
    #     st.dataframe(filtered_new_signups_checkout.drop_duplicates(subset="email"))

    #     # Téléchargement des nouveaux inscrit
    #     new_signups_checkout_xlsx = to_excel(filtered_new_signups_checkout, include_index=False)
    #     st.download_button(
    #         "Télécharger les données des nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté (.xlsx)",
    #         new_signups_checkout_xlsx,
    #         f"Nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté - ORIGINE : {customer_origine}, du {start_date} au {end_date}.xlsx",
    #         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )

    # # Afficher la plage de dates sélectionnée
    # st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

    ####################################################################################   Concentration des clients PAGES   #####################################################################

    # Créez une nouvelle page concentration des clients
    elif selected_page == "Concentration des clients par commune, Algérie":
        st.header("Concentration des clients par commune, Algérie")

        # Créez une liste des régions (wilayas) pour le filtre
        wilaya_list = geoloc_wilaya["wilaya"].unique()
        selected_wilaya = st.selectbox("Sélectionnez une wilaya :", wilaya_list)

        commune_counts = geoloc_wilaya["commune"].value_counts().reset_index()
        commune_counts.columns = ["commune", "nombre_clients"]

        commune_coordinates = (
            geoloc_wilaya.groupby("commune")
            .agg({"Latitude": "first", "Longitude": "first"})
            .reset_index()
        )
        commune_data = pd.merge(commune_coordinates, commune_counts, on="commune")
        region_data = geoloc_wilaya[["commune", "wilaya"]]

        merged_data = pd.merge(commune_data, region_data, how="left", on="commune")
        merged_data = merged_data.drop_duplicates(subset="commune")

        # Afficher les données filtrées
        show_merged_data = st.sidebar.checkbox("Afficher les données")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=True):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_merged_data:
            st.subheader("Nombre des Clients par Communes")
            st.dataframe(merged_data)

            # Bouton pour télécharger le DataFrame au format Excel
            merged_data_xlsx = to_excel(merged_data, include_index=False)
            st.download_button(
                "Télécharger les Orders en Excel (.xlsx)",
                merged_data_xlsx,
                "Nombre des Clients par Communes .xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Filtrer les données en fonction de la région (wilaya) sélectionnée
        filtered_data = merged_data[merged_data["wilaya"] == selected_wilaya]

        # Créez la carte en utilisant Plotly Graph Objects avec les données filtrées
        fig = go.Figure()

        # Définissez une échelle logarithmique pour ajuster la taille des cercles en fonction du nombre de clients
        min_size = 5  # Taille minimale des cercles
        max_size = 20  # Taille maximale des cercles
        min_clients = filtered_data["nombre_clients"].min()
        max_clients = filtered_data["nombre_clients"].max()

        fig = go.Figure()

        for i, row in filtered_data.iterrows():
            num_clients = row["nombre_clients"]
            # Appliquez une échelle logarithmique pour ajuster la taille en fonction du nombre de clients
            size = np.interp(
                np.log(num_clients),
                [np.log(min_clients), np.log(max_clients)],
                [min_size, max_size],
            )

            fig.add_trace(
                go.Scattermapbox(
                    lat=[row["Latitude"]],
                    lon=[row["Longitude"]],
                    mode="markers+text",
                    text=[
                        f'Commune: {row["commune"]}<br>Nombre de Clients: {num_clients}'
                    ],
                    marker=dict(size=size, sizemode="diameter", opacity=0.7),
                    name=row["commune"],
                )
            )

        fig.update_layout(
            title=f"Concentration des clients par commune, {selected_wilaya}",  # Mettez à jour le titre avec la wilaya sélectionnée
            autosize=True,
            hovermode="closest",
            mapbox=dict(
                style="carto-positron",
                bearing=0,
                center=dict(
                    lat=filtered_data[
                        "Latitude"
                    ].mean(),  # Centre sur la moyenne des latitudes des communes
                    lon=filtered_data[
                        "Longitude"
                    ].mean(),  # Centre sur la moyenne des longitudes des communes
                ),
                pitch=0,
                zoom=5,
            ),
            width=800,  # Largeur souhaitée en pixels
            height=600,  # Hauteur souhaitée en pixels
        )

        st.plotly_chart(fig)

    ####################################################################################   CSS STYLE   #####################################################################

    st.markdown(
        """
    <style>
    /* Styles pour la barre de navigation et le contenu principal */
    .css-1cypcdb.eczjsme11,
    .css-1wrcr25 {
        background-color: #ffffff !important; /* Fond blanc */
        border: 1px solid #FF6B05; /* Bordure de 1 pixel avec une couleur orange */
    }

    /* Styles pour le texte en noir */
    .css-k7vsyb h1,
    .css-nahz7x,
    .css-x78sv8,
    .css-q8sbsg,
    .css-1n76uvr.e1f1d6gn0 * {
        color: #000000 !important; /* Texte en noir */
    }

    /* Styles pour les boutons */
    .css-19rxjzo.ef3psqc11 {
        background-color: #ffffff !important; /* Couleur de fond orange par défaut */
        color: #FF6B05 !important; /* Couleur du texte en noir par défaut */
        border: 1px solid #FF6B05; /* Bordure noire de 1 pixel */
    }

    .css-19rxjzo.ef3psqc11:hover {
        background-color: #FF6B05 !important; /* Couleur de fond verte lorsque survolé */
        color: #ffffff !important; /* Couleur du texte en blanc lorsque survolé */
    }

    /* Styles pour le texte à l'intérieur des boutons */
    .css-19rxjzo.ef3psqc11 p {
        font-weight: bold;
        color: #FF6B05 !important;
    }

    .css-19rxjzo.ef3psqc11 p:hover {
        color: #ffffff !important; /* Couleur du texte en blanc lorsque survolé */
        font-weight: bold;
    }
    
    .ex0cdmw0 {
        color: #860102
    }

    .e1vs0wn31{
        background-color: #D4D4D4 !important; /*
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
