import tweepy
from textblob import TextBlob
import re
from azure.iot.device import IoTHubDeviceClient, Message
import matplotlib.pyplot as plot
import mysql.connector
from mysql.connector import errorcode

# Chaves de API do Twitter
consumer_key = "8CNb9XmJ7KrjOLp8UMNH9Fznh"
consumer_secret = "eGoW01Xjhg1rslbOk23UYIPBsItB6Qr0cOA4SDmiXGekWfPmDB"
access_token = "2973742181-XBE1qdfNncM8zrb5shvdKHLcJa1fvhvahPKQGjX"
access_token_secret = "qRGj0HK2MIKWZHlIuiZ2oConS7yNPIRQwrqgoP0sAyI10"

# Autenticação com a API do Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Função para dividir o texto em unidades léxicas
def tokenize(text):
    # Utilize expressões regulares para encontrar palavras, símbolos e pontuação
    tokens = re.findall(r'\w+|[^\w\s]', text)
    return tokens

def insert_db_grupo(tweet, sentiment):
    try:
        mydb = mysql.connector.connect(
            user= "roott",
            password= "Urubu100",
            host= "frequenciacardiaca.mysql.database.azure.com",
            database= "Grupo3"
        )

        if mydb.is_connected():
            #db_Info = mydb.get_server_info()
            #print("Conectado ao MySQL Server versão ", db_Info)

            mycursor = mydb.cursor()

            sql_query = "INSERT INTO analise_twitter(fraseTweet,sentimentoTweet,origem) VALUES (%s,%s,%s)"
            val = [tweet, sentiment, "Fernando"]
            mycursor.execute(sql_query, val)

            mydb.commit()

            print(mycursor.rowcount, "registro inserido")

    except mysql.connector.Error as e:
        print("Erro ao conectar com o MySQL", e)
    finally:
        if(mydb.is_connected()):
            mycursor.close()
            mydb.close()
            #print("Conexão com MySQL está fechada\n")

def insert_db(tweet, sentiment):
    try:
        mydb = mysql.connector.connect(
            user= "roott",
            password= "Urubu100",
            host= "sensor-movimento.mysql.database.azure.com",
            database= "movimento"
        )

        if mydb.is_connected():
            #db_Info = mydb.get_server_info()
            #print("Conectado ao MySQL Server versão ", db_Info)

            mycursor = mydb.cursor()

            sql_query = "INSERT INTO analise_twitter(fraseTweet,sentimentoTweet) VALUES (%s,%s)"
            val = [tweet, sentiment]
            mycursor.execute(sql_query, val)

            mydb.commit()

            print(mycursor.rowcount, "registro inserido")

    except mysql.connector.Error as e:
        print("Erro ao conectar com o MySQL", e)
    finally:
        if(mydb.is_connected()):
            mycursor.close()
            mydb.close()
            #print("Conexão com MySQL está fechada\n")            

# Palavras para pesquisar
palavras_pesquisa = ["sono",
                     "dormir",
                     "hospital"]

# Consulta de tweets
query = " ".join(palavras_pesquisa)
tweets = api.search_tweets(q=query, tweet_mode="extended", count=10)

# Análise léxica e análise de sentimento

for tweet in tweets:
    if hasattr(tweet, "retweeted_status"):
        cleaned_text = tweet.retweeted_status.full_text
    else:
        cleaned_text = tweet.full_text
    
    tokens = tokenize(cleaned_text)
    
    analysis = TextBlob(cleaned_text)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        sentiment = "positivo"
    elif polarity < 0:
        sentiment = "negativo"
    else:
        sentiment = "neutro"

    insert_db(cleaned_text, sentiment)
    insert_db_grupo(cleaned_text, sentiment)    
    
    # Verifica se alguma das palavras de pesquisa está presente no texto do tweet
    if any(palavra.lower() in cleaned_text.lower() for palavra in palavras_pesquisa):
        print(f"Tweet: {cleaned_text}")
        print("Unidades Léxicas:")
        for token in tokens:
            print(token)
        print(f"Sentimento: {sentiment}")
        print()
