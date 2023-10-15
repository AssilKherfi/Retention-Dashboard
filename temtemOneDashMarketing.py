# %%
import os
from datetime import datetime
from io import BytesIO
from io import StringIO
import json
import pandas as pd
import boto3
import bcrypt
import xlsxwriter
import re
from st_files_connection import FilesConnection
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import toml
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# %%
# Fonction pour charger les secrets depuis le fichier secrets.toml
def load_secrets():
    # Obtenez le chemin complet vers le fichier secrets.toml
    secrets_file_path = os.path.join(".streamlit", "secrets.toml")

    # Chargez le fichier secrets.toml
    secrets = toml.load(secrets_file_path)

    return secrets


# Fonction pour charger les données depuis S3 en utilisant les secrets du fichier toml
@st.cache_data
def load_data_from_s3_with_toml(secrets, bucket_name, file_name):
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
    conn = st.experimental_connection("s3", type=FilesConnection)
    return conn.read(
        f"{bucket_name}/{file_name}",
        input_format="csv",
        ttl=600,
        low_memory=False,
    )


# Fonction pour charger key_google.json depuis S3 en tant qu'objet JSON
def load_key_google_json_with_json_key(secrets, bucket_name, file_name):
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
    "csv_database/users_2023.csv",
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
        df_name = file_name.split("/")[-1].split(".")[0]  # Obtenir le nom du DataFrame
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
users = dataframes["users_2023"]

# %%
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 0)

orders["order_id"] = orders["order_id"].astype(str)
orders["customer_id"] = orders["customer_id"].astype(str)
orders["createdAt"] = pd.to_datetime(orders["createdAt"])
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

users["customer_id"] = users["customer_id"].astype(str)
users["customer_id"] = [
    re.sub(r"\.0$", "", customer_id) for customer_id in users["customer_id"]
]
users["tags"] = users["tags"].str.replace(r"\[|\]", "", regex=True)
users["tags"] = users["tags"].str.replace(r"['\"]", "", regex=True)
users = users[
    ~users["customer_id"].isin(
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

users["createdAt"] = pd.to_datetime(users["createdAt"])
users["createdAt"] = users["createdAt"].dt.strftime("%Y-%m-%d")
users["date"] = users["createdAt"]
users["date"] = pd.to_datetime(users["date"])
users = users.rename(columns={"Origine": "customer_origine"})
users_info = users[["date", "email", "phone", "customer_origine", "customer_id"]]


# key_google_json contient le contenu du fichier key_google.json que vous avez chargé depuis S3
creds = ServiceAccountCredentials.from_json_keyfile_dict(key_google_json)

# Autoriser l'accès à Google Sheets en utilisant les informations d'authentification
gc = gspread.authorize(creds)

# Ouvrez la feuille Google Sheets par son nom
spreadsheet_name = "Téléchargement"  # Remplacez par le nom de votre feuille
worksheet_name = (
    "telechargement"  # Remplacez par le nom de l'onglet que vous souhaitez lire
)

try:
    spreadsheet = gc.open(spreadsheet_name)
    worksheet = spreadsheet.worksheet(worksheet_name)
    # Lire les données de la feuille Google Sheets en tant que DataFrame pandas
    telechargement = pd.DataFrame(worksheet.get_all_records())

    # st.title("Lecture de la feuille Google Sheets")

    # # Affichez les données de la feuille Google Sheets en tant que tableau
    # st.table(telechargement)
except gspread.exceptions.SpreadsheetNotFound:
    st.error(
        f"La feuille '{spreadsheet_name}' ou l'onglet '{worksheet_name}' n'a pas été trouvé."
    )

# Liste des noms de feuilles
sheet_names = [
    "First_open_date_2020-2021",
    "First_open_date_2021-2022",
    "First_open_date_2022-2023",
]

# Initialiser une liste pour stocker les DataFrames
dfs = []

# Parcourir les feuilles et les stocker dans la liste
for sheet_name in sheet_names:
    spreadsheet = gc.open(sheet_name)

    # Accès à l'onglet spécifique
    worksheet = spreadsheet.worksheet("First_open_email")

    # Lire les données de l'onglet
    data = worksheet.get_all_values()

    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)

    # Définir la première ligne comme en-tête
    df.columns = df.iloc[0]
    df = df[1:]

    # Ajouter le DataFrame à la liste
    dfs.append(df)

# Concaténer tous les DataFrames dans un seul DataFrame
first_open_data = pd.concat(dfs, ignore_index=True).drop_duplicates(
    subset="email", keep="first"
)
new_signups_first_open_data = pd.merge(
    users_info, first_open_data, how="inner", on="email"
)

# st.dataframe(new_signups_first_open_data)

# Nom de la feuille et de l'onglet
sheet_name = "Utilisateur qui sont arrivé au Payment_method_screen"
worksheet_name = "Payment_method_screen"

# Ouverture de la feuille et de l'onglet spécifique
spreadsheet = gc.open(sheet_name)
worksheet = spreadsheet.worksheet(worksheet_name)

# Lire les données de l'onglet
data_payment_screen = worksheet.get_all_values()

# Créer un DataFrame à partir des données
payment_screen = pd.DataFrame(
    data_payment_screen[1:], columns=data_payment_screen[0]
)  # Utiliser la première ligne comme en-tête
payment_screen["Date de Payment method screen"] = pd.to_datetime(
    payment_screen["Date de Payment method screen"]
).dt.strftime("%Y-%m-%d")
payment_screen_users = pd.merge(users_info, payment_screen, how="inner", on="email")
order_payment_screen["date"] = pd.to_datetime(order_payment_screen["date"])

payment_screen_users_orders = pd.merge(
    payment_screen_users, order_payment_screen, on=["email", "date"], how="inner"
)
new_signups_checkout = payment_screen_users_orders[
    (payment_screen_users_orders["Status"] != "COMPLETED")
].drop_duplicates(subset="email", keep="last")

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

# Créez un dictionnaire user_db avec les informations d'utilisateur hachées
user_db = {
    user1_username: {
        "mot_de_passe": bcrypt.hashpw(user1_password.encode(), bcrypt.gensalt())
    },
    user2_username: {
        "mot_de_passe": bcrypt.hashpw(user2_password.encode(), bcrypt.gensalt())
    },
}


# Fonction de connexion
def login(user_db):
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


def verify_credentials(username, password):
    if username in user_db:
        hashed_password = user_db[username]["mot_de_passe"]
        return bcrypt.checkpw(password.encode(), hashed_password)
    return False


# Fonction pour appliquer les filtres
@st.cache_data
def apply_filters(df, customer_origine, business_cat, start_date, end_date):
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


def apply_filters_summary(df, customer_origine, start_date, end_date):
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
    st.title("Tableau de Bord TemtemOne")

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
        ["NOUVEAUX INSCRITS", "RETARGETING"],
    )

    ####################################################################################   NOUVEAUX INSCRITS PAGES   #####################################################################

    # Créez une nouvelle page Users
    if selected_page == "NOUVEAUX INSCRITS":
        st.header("NOUVEAUX INSCRITS 2023")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début", datetime(datetime.now().year, 1, 1).date()
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # Filtres
        customer_origine_options = ["Tous"] + list(users["customer_origine"].unique())
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        customer_country_options = ["Tous"] + list(users["customer_country"].unique())
        customer_country = st.sidebar.selectbox(
            "Customer Country", customer_country_options
        )

        # Appliquer les filtres
        filtered_new_signups = apply_filters_users(
            users,
            customer_origine,
            customer_country,
            # accountTypes,
            # tags,
            start_date,
            end_date,
        )

        filtered_data_download = apply_filters_summary(
            telechargement,
            customer_origine,
            start_date,
            end_date,
        )

        filtered_new_signups_first_open = apply_filters_summary(
            new_signups_first_open_data,
            customer_origine,
            start_date,
            end_date,
        )

        filtered_new_signups_first_open = (
            filtered_new_signups_first_open.drop_duplicates(subset="email")
        )

        # st.dataframe(filtered_new_signups_first_open)

        filtered_new_signups_checkout_data = apply_filters_summary(
            new_signups_checkout,
            customer_origine,
            start_date,
            end_date,
        )
        filtered_new_signups_checkout_data = (
            filtered_new_signups_checkout_data.drop_duplicates(subset="email")
        )
        # st.dataframe(filtered_new_signups_checkout_data)

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

        # Sélectionnez les nouveaux inscrits en fonction des filtres déjà appliqués
        new_signups = filtered_new_signups
        new_signups = new_signups[
            [
                "date",
                "customer_id",
                "lastName",
                "firstName",
                "phone",
                "email",
                "customer_country",
                "customer_origine",
            ]
        ]

        orders_users = orders.copy()
        orders_users = orders_users[orders_users["date"] >= "2023-01-01"]
        orders_users = orders_users[
            [
                "date",
                "customer_id",
                "order_id",
                "Status",
                "customer_origine",
                "businessCat",
                "total_amount_dzd",
            ]
        ].rename(columns={"customer_origine": "customer_origine_orders"})

        new_signups_copy = new_signups.copy()
        new_signups_copy = new_signups_copy.rename(
            columns={"date": "registration_date"}
        )

        new_signups_ordered = pd.merge(
            orders_users, new_signups_copy, how="inner", on="customer_id"
        )

        new_signups_ordered["New_status"] = new_signups_ordered["Status"]
        new_signups_ordered = new_signups_ordered.copy()
        new_signups_ordered["New_status"] = new_signups_ordered["New_status"].map(
            lambda x: "NOT COMPLETED" if x != "COMPLETED" else x
        )

        # Sélection de la granularité de la période
        granularity = st.radio(
            "Sélectionnez la période",
            ["Mois", "Semaine", "Jour"],
            key="granularity_users",
        )

        # Créez une nouvelle colonne "period" pour définir les cohortes en fonction de la granularité sélectionnée
        if granularity == "Jour":
            new_signups["period"] = new_signups["date"]
            period_duration = pd.Timedelta(days=1)  # Une journée
        elif granularity == "Semaine":
            new_signups["period"] = new_signups["date"] - pd.to_timedelta(
                (new_signups["date"].dt.dayofweek + 1) % 7, unit="D"
            )
            period_duration = pd.Timedelta(days=7)  # Une semaine
        else:
            new_signups["period"] = new_signups["date"].dt.strftime(
                "%Y-%m"
            )  # Convertir en format "YYYY-MM"
            period_duration = pd.Timedelta(days=31)  # Un mois

        # Agrégez les données par période (jour, semaine ou mois) et comptez le nombre total d'inscriptions pour le mois
        if period_duration:
            new_signups_count = (
                new_signups.groupby("period").size().reset_index(name="count")
            )
        else:
            new_signups_count = (
                new_signups.groupby("period")
                .size()
                .reset_index(name="count")
                .groupby("period")
                .sum()
                .reset_index()
            )

        # Assurez-vous que la dernière période se termine exactement à la fin de la période sélectionnée
        if len(new_signups_count) > 0 and period_duration:
            first_period_start = new_signups_count["period"].min()
            last_period_end = first_period_start + period_duration
            new_signups_count.loc[0, "period"] = last_period_end

        # Créez un graphique montrant le nombre de nouveaux inscrits par période
        if period_duration:
            period_label = f"{granularity}"
        else:
            period_label = "Mois"

        fig_nmb_usr = px.bar(
            new_signups_count,
            x="period",
            y="count",
            title=f"Nombre de Nouveaux Inscrits par {period_label}",
            labels={"period": period_label, "count": "Nombre de Nouveaux Inscrits"},
        ).update_xaxes(categoryorder="total ascending")

        # Créez une liste de toutes les catégories de business
        all_categories = ["Toutes les catégories"] + list(
            new_signups_ordered["businessCat"].unique()
        )

        selected_business_cat = st.selectbox(
            "Sélectionnez la catégorie de business", all_categories
        )

        # Dupliquez new_signups_ordered pour créer new_signups_ordered_copy
        new_signups_ordered_copy = new_signups_ordered.copy().drop_duplicates(
            subset="customer_id"
        )

        if selected_business_cat == "Toutes les catégories":
            filtered_new_signups_ordered = new_signups_ordered_copy
            filtered_new_signups_checkout = filtered_new_signups_checkout_data
        else:
            filtered_new_signups_ordered = new_signups_ordered_copy[
                new_signups_ordered_copy["businessCat"] == selected_business_cat
            ]
            filtered_new_signups_checkout = filtered_new_signups_checkout_data[
                filtered_new_signups_checkout_data["businessCat"]
                == selected_business_cat
            ]

        filtered_new_signups_ordered_customer = filtered_new_signups_ordered[
            "customer_id"
        ]
        filtered_new_signups_not_ordered = new_signups[
            ~new_signups["customer_id"].isin(filtered_new_signups_ordered_customer)
        ].drop_duplicates(subset="customer_id")
        filtered_new_signups_completed = filtered_new_signups_ordered[
            filtered_new_signups_ordered["New_status"] == "COMPLETED"
        ].drop_duplicates(subset="customer_id")
        filtered_new_signups_not_completed_customer = filtered_new_signups_completed[
            "customer_id"
        ]
        filtered_new_signups_not_completed = filtered_new_signups_ordered[
            ~filtered_new_signups_ordered["customer_id"].isin(
                filtered_new_signups_not_completed_customer
            )
        ].drop_duplicates(subset="customer_id")

        # Calculez les mesures directement sur les données filtrées
        total_filtered_downloads = filtered_data_download["Téléchargement"].sum()
        total_filtered_new_signups = len(
            new_signups.drop_duplicates(subset="customer_id")
        )
        total_filtered_new_signups_completed = len(filtered_new_signups_completed)
        total_filtered_new_signups_ordered = len(filtered_new_signups_ordered)
        total_filtered_new_signups_not_completed = len(
            filtered_new_signups_not_completed
        )
        total_filtered_new_signups_not_ordered = len(filtered_new_signups_not_ordered)
        total_filtered_new_signups_first_open = len(filtered_new_signups_first_open)
        total_filtered_new_signups_checkout = len(filtered_new_signups_checkout)

        # Créez un DataFrame avec les mesures calculées
        filtered_stats_data = pd.DataFrame(
            {
                "Mesure": [
                    "Nombre de Téléchargement",
                    "Nombre de Nouveaux Inscrit",
                    "Nombre de Nouveaux Inscrit qui n'ont jamais effectué une commande",
                    "Nombre de Nouveaux Inscrit qui ont overt la première fois l'app",
                    "Nombre de Nouveaux Inscrit qui ont effectué au moins une commande",
                    "Nombre de Nouveaux Inscrit qui ont effectué au moins un achat",
                    "Nombre de Nouveaux Inscrit qui n'ont n'ont jamais effectué un achat",
                    "Nombre de Nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté",
                ],
                "Valeur": [
                    total_filtered_downloads,
                    total_filtered_new_signups,
                    total_filtered_new_signups_not_ordered,
                    total_filtered_new_signups_first_open,
                    total_filtered_new_signups_ordered,
                    total_filtered_new_signups_completed,
                    total_filtered_new_signups_not_completed,
                    total_filtered_new_signups_checkout,
                ],
            }
        )

        # Utilisez les données calculées pour créer le graphique
        fig_filtered_stat = go.Figure(
            go.Bar(
                x=filtered_stats_data["Valeur"],
                y=filtered_stats_data["Mesure"],
                orientation="h",
                marker=dict(
                    color=[
                        "blue",
                        "green",
                        "red",
                        "purple",
                        "orange",
                        "yellow",
                        "brown",
                        "pink",
                    ]
                ),
                text=filtered_stats_data["Valeur"],
            )
        )

        # Personnalisez le graphique
        fig_filtered_stat.update_traces(texttemplate="%{text}", textposition="outside")

        # Personnalisez la mise en page
        fig_filtered_stat.update_layout(
            title=f"Statistiques des Nouveaux Inscrits - {selected_business_cat}",
            xaxis_title="Valeur",
            yaxis_title="Mesure",
        )

        # Créez des onglets pour basculer entre les deux visualisations
        selected_visualization = st.radio(
            "Sélectionnez la visualisation",
            ["Nombre de Nouveaux Inscrits", "Statistiques des Nouveaux Inscrits"],
        )

        if selected_visualization == "Nombre de Nouveaux Inscrits":
            # Affichez la heatmap de l'analyse de rétention
            st.plotly_chart(fig_nmb_usr)  # Utilisez le graphique fig_nmb_usr
        else:
            # Affichez la heatmap du nombre de clients
            st.plotly_chart(
                fig_filtered_stat
            )  # Utilisez le graphique fig_filtered_stat

        # Afficher et téléchargerles nouveaux inscrits dans le tableau de bord

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=False):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        # Fonction pour afficher un bouton de téléchargement en fonction d'une option sélectionnée
        def display_download_button(data, filename):
            data_xlsx = to_excel(data, include_index=False)
            st.download_button(
                f"Télécharger les données {filename} (.xlsx)",
                data_xlsx,
                f"{filename} - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Afficher les options de sélection dans la barre latérale
        selected_data_option = st.sidebar.selectbox(
            "Sélectionnez les données à télécharger",
            (
                "Nouveaux Inscrits",
                "Nouveaux Inscrits qui n'ont jamais effectué une commande",
                "Nouveaux Inscrits avec la date du first open app",
                "Nouveaux Inscrits qui ont effectué une commande",
                "Nouveaux Inscrits qui ont effectué au moins un achat",
                "Nouveaux Inscrits qui n'ont jamais effectué au moins un achat",
                "Nouveaux Inscrit qui sont arrivés au checkout et qui n'ont pas acheté",
            ),
        )

        # Vérifier l'option sélectionnée et afficher le bouton de téléchargement correspondant
        if selected_data_option == "Nouveaux Inscrits":
            display_download_button(new_signups, "Nouveaux Inscrits")
        elif (
            selected_data_option
            == "Nouveaux Inscrits qui n'ont jamais effectué une commande"
        ):
            display_download_button(
                filtered_new_signups_not_ordered,
                "Nouveaux Inscrits qui n'ont jamais effectué une commande",
            )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

    ####################################################################################   RETARGETING   #####################################################################

    # Créez une nouvelle page concentration des clients
    elif selected_page == "RETARGETING":
        st.header("RETARGETING")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        retargeting = orders.copy()
        # st.dataframe(retargeting)
        retargeting["New_status"] = retargeting["Status"].map(
            lambda x: "NOT COMPLETED" if x != "COMPLETED" else x
        )

        retargeting_completed = retargeting[retargeting["New_status"] == "COMPLETED"]
        retargeting_completed_customer = retargeting_completed["customer_id"]

        retargeting_not_completed = retargeting[
            ~retargeting["customer_id"].isin(retargeting_completed_customer)
        ].drop_duplicates(subset="customer_id", keep="last")

        st.dataframe(retargeting_not_completed)

        # Filtrer les clients selon les jours sélectionnés
        def filter_customers_by_last_purchase_days(
            retargeting_completed, days, customer_origine, businessCat
        ):
            current_date = pd.to_datetime("today")
            retargeting_completed["previous_order_date"] = pd.to_datetime(
                retargeting_completed["previous_order_date"]
            )
            if days == "Plus de 120":
                filtered_customers = retargeting_completed[
                    (
                        current_date - retargeting_completed["previous_order_date"]
                    ).dt.days
                    > 120
                ]
            else:
                filtered_customers = retargeting_completed[
                    (
                        current_date - retargeting_completed["previous_order_date"]
                    ).dt.days
                    <= days
                ]
            if "Tous" not in customer_origine:
                filtered_customers = filtered_customers[
                    filtered_customers["customer_origine"].isin(customer_origine)
                ]
            if "Tous" not in businessCat:
                filtered_customers = filtered_customers[
                    filtered_customers["businessCat"].isin(businessCat)
                ]
            return filtered_customers

        # Barre latérale pour sélectionner les jours du dernier achat, customer_origine et businessCat
        selected_days = st.sidebar.selectbox(
            "Sélectionnez les jours du dernier achat : ",
            [7, 14, 21, 30, 60, 90, 120, "Plus de 120"],
        )
        all_customer_origine = (
            retargeting_completed["customer_origine"].unique().tolist()
        )
        all_businessCat = retargeting_completed["businessCat"].unique().tolist()
        all_customer_origine.insert(0, "Tous")
        all_businessCat.insert(0, "Tous")
        selected_customer_origine = st.sidebar.multiselect(
            "Customer Origine (diaspora or Local)",
            all_customer_origine,
            default=["Tous"],
        )
        selected_businessCat = st.sidebar.multiselect(
            "Business catégorie", all_businessCat, default=["Tous"]
        )

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=False):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        def display_download_button_by_days(data, filename, selected_days):
            data_xlsx = to_excel(data, include_index=False)
            days = (
                "Plus de 120" if selected_days == "Plus de 120" else str(selected_days)
            )
            st.download_button(
                f"Télécharger les données {filename} {selected_days} derniers jours (.xlsx)",
                data_xlsx,
                f"{filename} - {days} jours.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Filtrer les clients selon les jours sélectionnés, customer_origine et businessCat
        if selected_days:
            # Pour les clients qui ont effectué un achat
            filtered_customers_completed = filter_customers_by_last_purchase_days(
                retargeting_completed,
                selected_days,
                selected_customer_origine,
                selected_businessCat,
            )

            filtered_df_completed = filtered_customers_completed[
                [
                    "date",
                    "customer_id",
                    "customer_username",
                    "customer_phone",
                    "customer_email",
                    "businessCat",
                    "customer_origine",
                ]
            ]

            filtered_df_last_purchase_completed = filtered_df_completed.drop_duplicates(
                subset="customer_id", keep="last"
            )
            st.write(
                f"Données des clients qui ont effectué leur dernier achat pendant {selected_days} derniers jours : ",
                filtered_df_last_purchase_completed,
            )

            # Télécharger les données en fonction de la durée sélectionnée pour les clients qui ont effectué un achat
            display_download_button_by_days(
                filtered_df_completed,
                "des clients qui ont effectué leur dernier achat pendant les",
                selected_days,
            )

            # Pour les clients qui n'ont pas complété d'achat
            filtered_customers_not_completed = filter_customers_by_last_purchase_days(
                retargeting_not_completed,
                selected_days,
                selected_customer_origine,
                selected_businessCat,
            )
            filtered_df_not_completed = filtered_customers_not_completed[
                [
                    "date",
                    "customer_id",
                    "customer_username",
                    "customer_phone",
                    "customer_email",
                    "businessCat",
                    "customer_origine",
                ]
            ]

            filtered_df_last_purchase_not_completed = (
                filtered_df_not_completed.drop_duplicates(
                    subset="customer_id", keep="last"
                )
            )
            st.write(
                f"Données des clients qui n'ont pas complété d'achat pendant {selected_days} derniers jours : ",
                filtered_df_last_purchase_not_completed,
            )

            # Télécharger les données en fonction de la durée sélectionnée pour les clients qui n'ont pas complété d'achat
            display_download_button_by_days(
                filtered_df_not_completed,
                "des clients qui n'ont pas complété d'achat pendant les",
                selected_days,
            )

            ################################################

            def filter_non_completed_customers_by_last_purchase_days(
                retargeting_not_completed, days, customer_origine, businessCat
            ):
                current_date = pd.to_datetime("today")
                retargeting_not_completed["previous_order_date"] = pd.to_datetime(
                    retargeting_not_completed["previous_order_date"]
                )
                if days == 7:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days
                        <= 6
                    ]
                elif days == 14:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(7, 13)
                    ]
                elif days == 21:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(14, 20)
                    ]

                elif days == 30:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(21, 29)
                    ]

                elif days == 60:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(30, 59)
                    ]

                elif days == 90:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(60, 89)
                    ]

                elif days == 120:
                    filtered_customers = retargeting_not_completed[
                        (
                            current_date
                            - retargeting_not_completed["previous_order_date"]
                        ).dt.days.between(90, 119)
                    ]

                if "Tous" not in customer_origine:
                    filtered_customers = filtered_customers[
                        filtered_customers["customer_origine"].isin(customer_origine)
                    ]
                if "Tous" not in businessCat:
                    filtered_customers = filtered_customers[
                        filtered_customers["businessCat"].isin(businessCat)
                    ]
                return filtered_customers

            # Utilisation de la fonction filter_non_purchasing_customers_by_last_purchase_days dans le code existant

            # Filtrer les clients non complétés selon les jours sélectionnés, customer_origine et businessCat
            if selected_days:
                filtered_non_completed_customers = (
                    filter_non_completed_customers_by_last_purchase_days(
                        retargeting_not_completed,
                        selected_days,
                        selected_customer_origine,
                        selected_businessCat,
                    )
                )
                filtered_non_completed_df = filtered_non_completed_customers[
                    [
                        "date",
                        "customer_id",
                        "customer_username",
                        "customer_phone",
                        "customer_email",
                        "businessCat",
                        "customer_origine",
                    ]
                ]
                st.write(
                    "DataFrame filtré des clients non complétés : ",
                    filtered_non_completed_df,
                )

                # Télécharger les données en fonction de la durée sélectionnée pour les clients non complétés
                display_download_button_by_days(
                    filtered_non_completed_df,
                    "des clients qui n'ont pas complété d'achat depuis",
                    selected_days,
                )

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
