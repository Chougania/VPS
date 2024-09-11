from pymongo import MongoClient, ASCENDING
import time

class BDD:

    def __init__(self):
        # Connexion à MongoDB
        self.client = MongoClient('localhost', 27017)
        # Création d'une nouvelle base de données
        self.db = self.client['searchengine']
        # Création d'une nouvelle collection dans la base de données
        self.collection = self.db['webpages']
        self.collection.create_index([("url", ASCENDING)], unique=True)
        self.last_visited = self.db['last_visited']
        self.last_visited.create_index([("url", ASCENDING)], unique=True)
        self.non_fr_en_urls = self.db['non_fr_en_urls']
        self.non_fr_en_urls.create_index([("url", ASCENDING)], unique=True)
        self.queue = self.db['queue']
        self.queue.create_index([("url", ASCENDING)], unique=True)
        self.miniqueue = self.db['miniqueue']
        self.miniqueue.create_index([("url", ASCENDING)], unique=True)
        self.crawled = self.db['crawled']
        self.crawled.create_index([("url", ASCENDING)], unique=True)

    def get_from_queue(self, how_many):
        from_queue = list(self.queue.find({"depth": {"$gt": 0}}, sort=[("add_time", 1)]).limit(how_many))
        for queued in from_queue:
            self.queue.delete_one({"_id": queued["_id"]})
        return from_queue

    def get_all_miniqueue(self):
        return list(self.miniqueue.find({"depth": {"$gt": 0}}, sort=[("add_time", 1)]))

    def get_all_visited(self):
        return list(self.last_visited.find({}))

    def fill_miniqueue(self):
        urls_a_transferer = self.queue.find({"depth": {"$gt": 0}}, sort=[("add_time", 1)]).limit(50000)
        for url in urls_a_transferer:
            self.miniqueue.insert_one(url)
            self.queue.delete_one({"_id": url["_id"]})

    def is_miniqueue_almost_empty(self):
        return self.miniqueue.count_documents({}) < 15000

    def add_to_queue(self, url, depth, add_time, domain):
        try:
            self.queue.insert_one({"url": url, "depth": depth, "add_time": add_time, "domain": domain})
        except Exception as e:
            ok = 'ok'
            #print(f"Impossible d'insérer l'URL en double : {e}")

    def get_miniqueue_size(self):
        return self.miniqueue.count_documents({})

    def get_queue_size(self):
        return self.queue.count_documents({})

    def pop_from_queue(self, delay):

        domaines_eligibles = list(self.last_visited.find(
            {"last_visit": {"$lt": time.time()-delay}},
            {"url": 1, "_id": 0}
        ))

        liste_domaines = [domaine['url'] for domaine in domaines_eligibles]

        if len(liste_domaines) > 0:

            next_url = self.miniqueue.find_one(
                {"domain": {"$in": liste_domaines}, "depth": {"$gt": 0}},
                sort=[("add_time", 1)]
            )

            if next_url:
                self.miniqueue.delete_one({"_id": next_url["_id"]})
                return next_url["url"], next_url["depth"]
            else:
                next_url = self.miniqueue.find_one(
                    {"domain": {"$in": liste_domaines}},
                    sort=[("add_time", 1)]
                )
                if next_url:
                    self.miniqueue.delete_one({"_id": next_url["_id"]})
                    return next_url["url"], next_url["depth"]
                return None, None

        else:
            return None, None


    def save_page_data(self, page_data):
        # Insérer les données de la page dans MongoDB
        try:
            self.collection.insert_one(page_data)
        except Exception as e:
            print(f"Impossible d'insérer l'URL en double : {e}")


    def check_if_not_fr_en(self, url):
        return self.non_fr_en_urls.find_one({"url": url}) is not None

    def add_to_not_fr_en(self, url):
        try:
            self.non_fr_en_urls.insert_one({"url": url})
        except Exception as e:
            print(f"{e}")


    def check_if_crawled(self, url):
        #return self.crawled.find_one({"url": url}) is not None
        return self.collection.find_one({"url": url}) is not None

    def add_to_crawled(self, url):
        try:
            self.crawled.insert_one({"url": url})
        except Exception as e:
            print(f"{e}")

    def get_last_visited(self, url):
        last_visited = self.last_visited.find_one({"url": url})
        if last_visited:
            return last_visited["last_visit"]
        else:
            return None

    def add_to_last_visited(self, url, time):
        try:
            self.last_visited.insert_one({'url': url, 'last_visit': time})
        except Exception as e:
            ok = 'ok'
            #print(f"{e}")

    def update_to_last_visited(self, url, time):
        try:
            filtre = {"url": url}
            # Mises à jour à appliquer
            mise_a_jour = {"$set": {"last_visit": time}}
            # Mise à jour du premier document correspondant au filtre
            resultat = self.last_visited.update_one(filtre, mise_a_jour)
        except Exception as e:
            print(f"{e}")