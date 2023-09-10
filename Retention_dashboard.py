# %%
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from operator import attrgetter
from datetime import datetime, timedelta
import os
import boto3
import openpyxl
from io import StringIO
from io import BytesIO
import bcrypt
import xlsxwriter


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
            "310179181696" "878446",
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
            "548088380288" "3526036",
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
    st.title("Tableau de Bord de Retention")

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

    # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        format = workbook.add_format({"num_format": "0.00"})
        worksheet.set_column("A:A", None, format)
        writer.save()
        processed_data = output.getvalue()
        return processed_data

    # Bouton pour télécharger le DataFrame au format Excel
    if st.button("Télécharger les Orders au format Excel"):
        filtered_data_xlsx = to_excel(filtered_data)
        st.download_button(
            label="📥 Télécharger Orders",
            data=filtered_data_xlsx,
            file_name="Orders.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

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

    filtered_data["order_period"] = filtered_data["date"].dt.to_period(period_frequency)
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

    # Calculer les clients qui ont abandonné (churn) pour chaque cohort
    churned_customers = cohort_pivot.copy()
    churned_customers.iloc[:, 1:] = (
        cohort_pivot.iloc[:, 1:].values - cohort_pivot.iloc[:, :-1].values
    )
    churned_customers.columns = [f"Churn_{col}" for col in churned_customers.columns]

    # Calculer la rétention en pourcentage
    retention_percentage = cohort_pivot.divide(cohort_pivot.iloc[:, 0], axis=0) * 100

    # Afficher la matrice de rétention
    st.subheader("Matrice de Rétention")
    st.dataframe(retention_percentage)

    # # Téléchargement de la  Rétention
    # if st.download_button(
    #     "Télécharger la Rétention en Excel (.xlsx)",
    #     retention_percentage.to_excel,
    #     "Retention.xlsx",
    #     args=(True,),
    #     key="download_retention",
    # ):
    #     st.success("La Rétention téléchargée avec succès !")

    # Renommer les colonnes de la matrice de rétention
    cohort_pivot.columns = [
        f"Retention_{str(col).zfill(2)}" for col in cohort_pivot.columns
    ]

    # Concaténer la matrice de rétention avec les clients qui ont abandonné
    cohort_analysis = pd.concat([cohort_pivot, churned_customers], axis=1)

    # Afficher la matrice de rétention mise à jour
    st.subheader("Matrice de Rétention avec Churn")
    st.dataframe(cohort_analysis)

    # # Téléchargement de la rétention avec churn
    # if st.download_button(
    #     "Télécharger la Rétention avec Churn en Excel (.xlsx)",
    #     cohort_analysis.to_excel,
    #     "retention_chrun.xlsx",
    #     args=(True,),
    #     key="download_retention_churn",
    # ):
    #     st.success("La Rétention avec Churn téléchargée avec succès !")

    # Afficher la heatmap de la matrice de rétention de la rétention en pourcentage

    st.subheader("Heatmap de la Matrice de Rétention (Rétention en %)")
    plt.figure(figsize=(10, 6))
    ax = sns.heatmap(
        retention_percentage, annot=True, cmap="YlGnBu", fmt=".1f", cbar=False
    )

    for t in ax.texts:
        t.set_text(f"{float(t.get_text()):.1f}%")
    plt.title("Heatmap de la Matrice de Rétention (Rétention en %)")
    plt.xlabel("Période")
    plt.ylabel("Cohorte")

    # # Téléchargement de l'image de la heatmap de la retention
    # if st.button("Télécharger l'image de la Heatmap (Rétention en %)"):
    #     plt.savefig("heatmap_matrice_de_retention.png")
    #     st.success("Image de la Heatmap (Rétention en %) téléchargée avec succès !")

    # Afficher la heatmap de la matrice de rétention du churn en pourcentage
    st.subheader("Heatmap de la Matrice de Rétention (Churn en %)")
    plt.figure(figsize=(10, 6))
    ax = sns.heatmap(
        churned_customers.divide(cohort_pivot.iloc[:, 0], axis=0) * 100,
        annot=True,
        cmap="YlGnBu",
        fmt=".1f",
        cbar=False,
    )

    for t in ax.texts:
        t.set_text(f"{float(t.get_text()):.1f}%")
    plt.title("Heatmap de la Matrice de Rétention (Churn en %)")
    plt.xlabel("Période")
    plt.ylabel("Cohorte")

    # # Téléchargement de l'image de la heatmap du churn
    # if st.button("Télécharger l'image de la Heatmap (Churn en %)"):
    #     plt.savefig("heatmap_matrice_de_retention_churn.png")
    #     st.success("Image de la Heatmap (Churn en %) téléchargée avec succès !")

    st.markdown(
        """
        <style>
        .css-1cypcdb.eczjsme11 { /* Classe CSS spécifique pour le barre de navigation */
            background-color: #ffffff !important; /* Couleur bleue */;
            border: 1px solid #FF6B05; /* Bordure de 1 pixel avec une couleur orange */
        }
        .css-1wrcr25 { /* Conteneur du contenu principal */
            background-color: #ffffff !important; /* Fond blanc */
            border: 1px solid #FF6B05; /* Bordure de 1 pixel avec une couleur orange */
        }
        .css-k7vsyb h1 {
            color: #000000 !important; /* Texte en noir */
        }

        .css-nahz7x{
            color: #000000 !important; /* Texte en noir */
        }

        .css-x78sv8 {
            color: #000000; /* Couleur du texte en noir */
        }
        .css-q8sbsg{
            color: #000000; /* Couleur du texte en noir */
        }

        .css-1n76uvr.e1f1d6gn0 * { /* Tous les éléments enfants du conteneur */
            color: #000000 !important; /* Texte en noir */
        }

        /* Cible les boutons avec la classe .css-19rxjzo.ef3psqc11 */
        .css-19rxjzo.ef3psqc11 {
            background-color: #FF6B05 !important; /* Couleur de fond verte */@
        }
        
        /* Cible le texte à l'intérieur des boutons */
        .css-19rxjzo.ef3psqc11 p {
            color: #000000 !important; /* Couleur du texte en noir */
            font-weight:bold;
        }

        /* Cible le bouton par son attribut data-testid */
        button[data-testid="StyledFullScreenButton"] {
            background-color: #FF6B05 !important; /* Couleur de fond vert */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
