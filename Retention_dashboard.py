# %%
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from operator import attrgetter
from datetime import datetime, timedelta
import os
import boto3
from io import StringIO
import bcrypt


# %%
# Fonction pour télécharger et charger un DataFrame depuis une URL S3
@st.cache_data  # Ajoutez le décorateur de mise en cache
def load_data_s3(bucket_name, file_name):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    object_content = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(object_content), delimiter=",", low_memory=False)


# Accéder aux secrets de la section "s3_credentials"
s3_secrets = st.secrets["s3_credentials"]

# Créer une session AWS
session = boto3.Session(
    aws_access_key_id=s3_secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=s3_secrets["AWS_SECRET_ACCESS_KEY"],
)

# Créer un client S3
s3_client = session.client("s3")

# Nom du seau S3
bucket_name = "one-data-lake"

# Liste des noms de fichiers à télécharger depuis S3
file_names = [
    "csv_database/orders.csv",
    # "csv_database/users.csv",
]

# Dictionnaire pour stocker les DataFrames correspondants aux fichiers
dataframes = {}

# Télécharger et traiter les fichiers
for file_name in file_names:
    df_name = file_name.split("/")[-1].split(".")[0]  # Obtenir le nom du DataFrame
    dataframes[df_name] = load_data_s3(bucket_name, file_name)

# Créer un DataFrame à partir des données
orders = dataframes["orders"]
# users = dataframes["users"]

# %%
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 0)

# orders = pd.read_csv("orders.csv", delimiter=",", low_memory=False)
orders["order_id"] = orders["order_id"].astype(str)
orders["customer_id"] = orders["customer_id"].astype(str)
orders["createdAt"] = pd.to_datetime(orders["createdAt"])
orders = orders.rename(columns={"job_status": "Status"})
orders = orders[~orders["Status"].isin(["ABANDONED"])]
orders["customer_id"] = orders["customer_id"].str.rstrip(".0")
orders["businessCat"] = orders["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
)
orders["customer_origine"] = orders["paymentType"].apply(
    lambda x: "Diaspora" if x == "CARD_PAY" else "Local"
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
orders = orders.rename(columns={"Order Type": "Order_Type"})
orders.loc[(orders["customer_id"] == "73187559488.0"), "Order_Type"] = "EXTERNE"

orders_pmi = orders[orders["Order_Type"] == "EXTERNE"]

# users = pd.read_csv("users.csv", delimiter=",", low_memory=False)
# users["customer_id"] = users["customer_id"].astype(str)
# users["createdAt"] = pd.to_datetime(users["createdAt"])
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

# Variables d'état pour suivre si l'utilisateur est connecté
is_authenticated = False
current_user = None


# Page de connexion
def login_page():
    st.title("Connexion")
    username = st.text_input("Nom d'utilisateur", key="username")
    password = st.text_input("Mot de passe", type="password", key="password")

    global is_authenticated, current_user

    if st.button("Se connecter"):
        if username in user_db and bcrypt.checkpw(
            password.encode(), user_db[username]["mot_de_passe"]
        ):
            is_authenticated = True
            current_user = username
            st.success(f"Connexion réussie en tant que {username}")
            return True  # Connexion réussie, renvoie True
        else:
            st.error("Nom d'utilisateur ou mot de passe incorrect. Veuillez réessayer.")
            return False  # Connexion échouée, renvoie False


# Page protégée
def protected_page(username):
    st.title(f"Tableau de bord de {username}")

    # Contenu de la page protégée
    st.write("Bienvenue dans votre tableau de bord protégé !")


# Fonction pour appliquer les filtres
@st.cache_data
def apply_filters(df, status, customer_origine, business_cat, time_period, num_periods):
    filtered_data = df.copy()

    if status != "Tous":
        filtered_data = filtered_data[filtered_data["Status"] == status]

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if business_cat != "Toutes":
        filtered_data = filtered_data[filtered_data["businessCat"] == business_cat]

    date_col = "date"

    # Calculer la date de début de la période en fonction du nombre de périodes souhaitées
    if time_period == "Semaine":
        period_type = "W"
        start_date = filtered_data[date_col].max() - pd.DateOffset(weeks=num_periods)
    else:
        period_type = "M"
        start_date = filtered_data[date_col].max() - pd.DateOffset(months=num_periods)

    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date)
        & (filtered_data[date_col] <= filtered_data[date_col].max())
    ]

    return filtered_data.copy()


# Fonction pour calculer la plage de dates
def get_date_range(filtered_data, time_period, num_periods):
    end_date = filtered_data["date"].max()

    if time_period == "Semaine":
        start_date = end_date - pd.DateOffset(weeks=num_periods)
    else:
        start_date = end_date - pd.DateOffset(months=num_periods)

    return start_date, end_date


# Créer une application Streamlit
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Sélectionnez une page", ["Connexion", "Tableau de bord"]
    )

    if is_authenticated:  # Vérifiez si l'utilisateur est authentifié ici
        if page == "Connexion":
            page = "Tableau de bord"

    if page == "Tableau de bord":
        protected_page(current_user)
        st.title("Tableau de bord d'analyse de Cohorte")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection de la période
        time_period = st.sidebar.radio("Période", ["Semaine", "Mois"])

        # Sélection du nombre de périodes précédentes
        if time_period == "Semaine":
            num_periods_default = 4  # Par défaut, sélectionner 4 semaines
        else:
            num_periods_default = 6  # Par défaut, sélectionner 6 mois

        num_periods = st.sidebar.number_input(
            "Nombre de périodes précédentes", 1, 36, num_periods_default
        )

        # Filtres
        status_options = ["Tous"] + list(orders["Status"].unique())
        status = st.sidebar.selectbox("Statut", status_options)

        customer_origine_options = ["Tous", "Diaspora", "Local"]
        customer_origine = st.sidebar.selectbox(
            "Choisissez le type de client (Diaspora ou Local)", customer_origine_options
        )

        business_cat_options = ["Toutes"] + list(orders["businessCat"].unique())
        business_cat = st.sidebar.selectbox("Business catégorie", business_cat_options)

        # Appliquer les filtres
        filtered_data = apply_filters(
            orders,
            status,
            customer_origine,
            business_cat,
            time_period,
            num_periods,
        )

        # Afficher les données filtrées
        show_filtered_data = st.sidebar.checkbox("Afficher les données")

        if show_filtered_data:
            st.subheader("Data Orders")
            st.dataframe(filtered_data)

            # Téléchargement des données
            st.subheader("Téléchargement de orders")
            download_format = st.radio(
                "Choisir le format de téléchargement :", ["Excel (.xlsx)", "CSV (.csv)"]
            )

            if st.button("Télécharger les données orders"):
                if download_format == "Excel (.xlsx)":
                    filtered_data.to_excel("orders.xlsx", index=False)
                elif download_format == "CSV (.csv)":
                    filtered_data.to_csv("orders.csv", index=False)

        # Afficher la plage de dates sélectionnée
        start_date, end_date = get_date_range(filtered_data, time_period, num_periods)
        st.sidebar.write(
            f"Plage de dates sélectionnée : {start_date.strftime('%d-%m-%Y')} à {end_date.strftime('%d-%m-%Y')}"
        )

        # Calculer et afficher l'analyse de cohorte
        st.subheader("Analyse de Cohorte")
        filtered_data.dropna(subset=["customer_id"], inplace=True)
        filtered_data["date"] = pd.to_datetime(filtered_data["date"])

        period_frequency = "W" if time_period == "Semaine" else "M"

        filtered_data["order_period"] = filtered_data["date"].dt.to_period(
            period_frequency
        )
        filtered_data["cohort"] = (
            filtered_data.groupby("customer_id")["date"]
            .transform("min")
            .dt.to_period(period_frequency)
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

        cohort_size = cohort_pivot.iloc[:, 0]
        retention_matrix = cohort_pivot.divide(cohort_size, axis=0)

        # Afficher la matrice de rétention
        st.subheader("Matrice de Rétention")
        st.dataframe(retention_matrix)

        # Téléchargement de la matrice de rétention
        st.subheader("Téléchargement de la Matrice de Rétention")
        if st.button("Télécharger la Matrice de Rétention en Excel (.xlsx)"):
            retention_matrix.to_excel("matrice_de_retention.xlsx", index=True)

        # Afficher la heatmap de la matrice de rétention
        st.subheader("Heatmap de la Matrice de Rétention")
        plt.figure(figsize=(10, 6))
        sns.heatmap(retention_matrix, annot=True, cmap="YlGnBu", fmt=".0%")
        plt.title("Heatmap de la Matrice de Rétention")
        plt.xlabel("Période")
        plt.ylabel("Cohorte")
        st.pyplot(plt)

        # Téléchargement de l'image de la heatmap
        if st.button("Télécharger l'image de la Heatmap"):
            plt.savefig("heatmap_matrice_de_retention.png")
            st.success("Image de la Heatmap téléchargée avec succès !")

        st.markdown(
            """
        <style>
        .css-1cypcdb.eczjsme11 { /* Classe CSS spécifique pour le barre de navigation */
            background-color: #0A3781 !important; /* Couleur bleue */
        }
        .css-1wrcr25 { /* Conteneur du contenu principal */
            background-color: #70a8ba !important; /* Fond blanc */
        }
        
        .css-1n76uvr.e1f1d6gn0 * { /* Tous les éléments enfants du conteneur */
            color: #000000 !important; /* Texte en noir */
        }

        /* Cible les boutons avec la classe .css-19rxjzo.ef3psqc11 */
        .css-19rxjzo.ef3psqc11 {
            background-color: #068863 !important; /* Couleur de fond verte */
        }
        
        /* Cible le texte à l'intérieur des boutons */
        .css-19rxjzo.ef3psqc11 p {
            color: #000000 !important; /* Couleur du texte en noir */
            font-weight:bold;
        }

        /* Cible le bouton par son attribut data-testid */
        button[data-testid="StyledFullScreenButton"] {
            background-color: #068817 !important; /* Couleur de fond vert */
        }
        </style>
        """,
            unsafe_allow_html=True,
        )

    elif page == "Connexion":
        if login_page():
            st.sidebar.success("Vous êtes connecté en tant que " + current_user)
            st.sidebar.info(
                "Accédez au tableau de bord en sélectionnant 'Tableau de bord' dans la barre latérale."
            )

        st.markdown(
            """
        <style>
        .css-1cypcdb.eczjsme11 { /* Classe CSS spécifique pour le barre de navigation */
            background-color: #068863 !important; /* Couleur bleue */
        }
        .css-1wrcr25 { /* Conteneur du contenu principal */
            background-color: #70a8ba !important; /* Fond blanc */
        }
        
        .css-1n76uvr.e1f1d6gn0 * { /* Tous les éléments enfants du conteneur */
            color: #EFEFEF !important; /* Texte en noir */
        }

        /* Cible les boutons avec la classe .css-19rxjzo.ef3psqc11 */
        .css-19rxjzo.ef3psqc11 {
            background-color: #0A3781 !important; /* Couleur de fond verte */
        }
        
        /* Cible le texte à l'intérieur des boutons */
        .css-19rxjzo.ef3psqc11 p {
            color: #000000 !important; /* Couleur du texte en noir */
            font-weight:bold;
        }

        .st-ee {
        background-color: #ff9999; /* Couleur de fond rouge pour l'erreur */
        padding: 10px;
        border-radius: 5px;
        text-align: center;
        }
        .st-ee p {
            color: #ff0000; /* Couleur du texte en rouge */
            font-weight: bold;
        }

        </style>
        """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
