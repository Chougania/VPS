from pymongo import MongoClient, ASCENDING
import re

class BDD:

    def __init__(self):
        # Connexion à MongoDB
        self.client = MongoClient('localhost', 27017)
        # Création d'une nouvelle base de données
        self.db = self.client['v2fsearchengine']
        # Création d'une nouvelle collection dans la base de données
        self.webpages = self.db['webpages']
        self.mots_texte = self.db['mots_texte']
        self.mots_titles = self.db['mots_titles']

    def get_urls_with_word_and_their_number(self, word):
        mot = self.mots_texte.find_one({"mot": word})
        if not mot:
            return None, None
        return mot, len(mot.get("appear_in"))

    def get_all_pages_word_counter(self):
        results = self.webpages.find({}, {"url": 1, "nb_mots": 1, "_id": 0})
        return {doc['url']: doc['nb_mots'] for doc in results}
    
    def get_info_for(self, url):
        website = self.webpages.find_one({"url": url})
        if not website:
            return website
        return website

    def get_count_of_webpages(self):
        return self.webpages.count_documents({})

    def how_many_words_in_text_url(self, url):
        doc = self.webpages.find_one({"url": url})
        if doc:
            return doc.get("nb_mots")
        else:
            # A priori impossible que ça arrive...
            return 1000000000000000000000

    def get_pagerank_for(self, url):
        website = self.webpages.find_one({"url": url})
        if not website:
            return 0
        return website.get("pageRank")