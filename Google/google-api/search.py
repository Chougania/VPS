from bdd import BDD
import math
import re
import time
import threading

class Search:

	def __init__(self):
		self.bdd = BDD()
		self.PONDERATION_TF_IDF = 0.7
		self.PONDERATION_PAGERANK = 0.3
		self.number_of_pages_indexed = self.bdd.get_count_of_webpages()
		self.lock = threading.Lock()


	def search_term(self, term):
		term = term.lower()
		# Trouver les documents contenant le mot
		urls_contenant_mot, nombre_urls_avec_mot = self.bdd.get_urls_with_word_and_their_number(term)
		tf_idf_scores = {}

		if not nombre_urls_avec_mot:
			return tf_idf_scores

		# Calculer IDF une seule fois pour le mot
		idf = math.log(self.number_of_pages_indexed / nombre_urls_avec_mot)

		all_webpages = self.bdd.get_all_pages_word_counter()

		# Calculer TF-IDF pour chaque document contenant le mot
		for occ in urls_contenant_mot.get('appear_in'):
		    url = occ['url']
		    count_words_in_url = all_webpages[url]
		    tf = occ['occurrences'] / count_words_in_url
		    tf_idf = tf * idf
		    tf_idf_scores[url] = tf_idf

		return tf_idf_scores

	def combine_with_pagerank(self, websites):
		combined_scores = {}

		for website, score in websites.items():
			website_infos = self.bdd.get_info_for(website)
			combined_scores[website] = [self.PONDERATION_TF_IDF * score + self.PONDERATION_PAGERANK * website_infos.get("pageRank"), website_infos.get("titles"), website_infos.get("url")]
		return combined_scores

	def process_term(self, term, all_tf_idf):
		tf_idf = self.search_term(term)
		with self.lock:
		    all_tf_idf.append(tf_idf)

	def search_terms(self, query, limit):
		query = query.replace("œ", "oe")
		query = re.sub(r"[^a-zA-ZÀ-ÿ ']", " ", query)

		all_tf_idf = []
		threads = []
		for term in query.split():
		    thread = threading.Thread(target=self.process_term, args=(term, all_tf_idf))
		    thread.start()
		    threads.append(thread)

		for thread in threads:
		    thread.join()

		final_scores = {}
		for tf_idf in all_tf_idf:
			for url, score in tf_idf.items():
				if url not in final_scores:
					final_scores[url] = score
				else:
					final_scores[url] += score

		return dict(sorted(final_scores.items(), key=lambda x: x[1], reverse=True)[:limit])

		# Trier les scores TF-IDF dans l'ordre décroissant
		final_scores = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
		return final_scores

	def search(self, query, limit=20):
		debut = time.time()
		tf_idf = self.search_terms(query, limit)
		print(f"search terms prend {time.time() - debut} secondes")
		debut = time.time()
		results = sorted(self.combine_with_pagerank(tf_idf).items(), key=lambda x: x[1], reverse=True)
		print(f"pagerank prend {time.time() - debut} secondes")
		return results