import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
import threading
import time
import re
import tldextract
from bdd import BDD
from datetime import datetime
import socket

class Crawler:
    def __init__(self, urls, max_threads, request_delay=1):
        self.lock = threading.Lock()
        self.max_threads = max_threads
        self.request_delay = request_delay
        self.user_agent = "v2fCrawlBot/1.0"
        self.last_visited = {}
        self.bdd = BDD()
        self.miniqueue = self.bdd.get_all_miniqueue()
        self.get_all_ex_last_visited()
        self.crawled = []
        for url in urls:
            self.bdd.add_to_queue(url[0], url[1], datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.get_domain(url[0]))
            self.add_to_last_visited(self.get_domain(url[0]), 0)

    def get_all_ex_last_visited(self):
        visited = self.bdd.get_all_visited()
        for domaine in visited:
            self.last_visited[domaine["url"]] = domaine["last_visit"]

    def fill_miniqueue(self):
        self.miniqueue = self.miniqueue + self.bdd.get_from_queue(40000)

    def pop_from_queue(self):
        domaines_eligibles = [domaine for domaine, temps in self.last_visited.items() if temps < (time.time() - self.request_delay)]
        for i in range(len(self.miniqueue)):
            if (self.miniqueue[i]["domain"] in domaines_eligibles):
                page = self.miniqueue.pop(i)
                return page["url"], page["depth"]
        return None, None

    def add_to_last_visited(self, url, time):
        self.last_visited[url] = time

    def get_last_visited(self, url):
        return self.last_visited.get(url)

    def update_to_last_visited(self, url, time):
        self.last_visited[url] = time

    def clean_text(self, text):
        # Supprimer les sauts de ligne, etc.
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def has_language_marker(self, url):
        # Regex pour détecter des motifs de langue, mais ignorer "fr" et "en"
        # La regex suivant cherche des séquences de deux lettres qui ne sont pas 'fr' ou 'en'
        pattern = r'/(?!(fr\b|en\b))[a-z]{2}/|\b(?!(fr\b|en\b))[a-z]{2}\.'
        return re.search(pattern, url) is not None

    def can_fetch(self, url):
        domain = urlparse(url).netloc
        try:
            robot_url = urljoin(url, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robot_url)
            rp.read()
            return rp.can_fetch(self.user_agent, url)
        except Exception as e:
            print(f"Une erreur s'est produite lors de l'accès au robots.txt de {url}: {e}")
            return False

    def get_domain(self, url):
        extract_result = tldextract.extract(url)
        domain = "{}.{}".format(extract_result.domain, extract_result.suffix)
        return domain

    def crawl_page(self, url, depth):
        
        if not self.can_fetch(url):
            print(f"Accès refusé par robots.txt : {url}")
            return

        try:
            print(f"Je crawl ce site : {url}")
            response = requests.get(url,headers={'User-Agent': self.user_agent}, timeout=5)
            if response.status_code != 200:
                return
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # On vérifie que c'est bien en français ou en anglais
            html_lang = soup.find('html').get('lang', '').lower()
            if 'fr' not in html_lang and 'en' not in html_lang:
                print(f"URL non française ou anglaise : {url}")
                return
            
            title = soup.find('title').text if soup.find('title') else ''
            h1_tags = [self.clean_text(h1.text) for h1 in soup.find_all('h1')]
            h2_tags = [self.clean_text(h2.text) for h2 in soup.find_all('h2')]
            text = self.clean_text(soup.get_text())

            base_url = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(url))
            links = soup.find_all('a')
            all_valid_links = []
            for link in links:
                href = link.get('href')
                if href and not href.startswith('http') and href.startswith('/'):
                    href = urljoin(base_url, href)
                if href and href.startswith('http') and not href.endswith('.pdf'):
                    if not self.has_language_marker(href) and urlparse(href).netloc:
                        
                        all_valid_links.append(href)
                        if href not in self.crawled:
                            last_visit_time = self.get_last_visited(self.get_domain(href))
                            if not last_visit_time:
                                self.add_to_last_visited(self.get_domain(href), 0)
                            self.bdd.add_to_queue(href, depth - 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.get_domain(href))
            
            
            page_data = {
                'url': url,
                'lang': html_lang,
                'titles': title,
                'h1': h1_tags,
                'h2': h2_tags,
                'text': text,
                'links_to': all_valid_links,
                'linked_in': [],
                'pagerank': 0,
                'indexed': False,
                'crawled_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self.bdd.save_page_data(page_data)
            #self.data.append({'url': url, 'lang': html_lang, 'title': title, 'h1': h1_tags, 'h2': h2_tags, 'metadata': metadata, 'text': text, 'links': all_valid_links})

        except Exception as e:
            print(f"Une erreur s'est produite lors de l'accès à {url}: {e}")

    def crawl(self):
        while True:
            #debut2 = time.time()
            with self.lock:
                #print(threading.get_native_id())
                if len(self.miniqueue) < 20000:
                    self.fill_miniqueue()
                url, depth = self.pop_from_queue()
                if url == None:
                    continue
                if depth == 0:
                    continue
                domain = self.get_domain(url)
                self.update_to_last_visited(domain, time.time())
                #print(f"selection faite en {str(time.time() - debut)}")

            #debut2 = time.time()
            if url not in self.crawled:
                self.crawled.append(url)
                self.crawl_page(url, depth)
            #print(f"crawl fait en {str(time.time() - debut2)}")

    def start(self):
        threads = []
        for _ in range(self.max_threads):
            t = threading.Thread(target=self.crawl)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

#crawler = Crawler([("https://www.exemple1.com", 2), ("https://www.exemple2.com", 2), ("https://www.exemple3.com", 2)], depth=2, max_threads=5, request_delay=10)

#print(crawler.crawl_page("https://en.wikipedia.org", 2))

#collected_data = crawler.get_data()
socket.setdefaulttimeout(5)
urls = [("https://fr.wikipedia.org/wiki/YouTube", 6),
        ("https://www.lemonde.fr/", 3),
        ("https://www.lefigaro.fr/", 3),
        ("https://www.liberation.fr/", 3),
        ("https://france3-regions.francetvinfo.fr/provence-alpes-cote-d-azur/alpes-maritimes/nice/carnaval-de-nice-2024-cyprien-star-de-youtube-devoile-le-dessin-de-son-char-2900054.html", 3),
        ("https://www.education.gouv.fr/", 2)]
crawler = Crawler(urls, max_threads=100, request_delay=10)
crawler.start()


"""

[("https://fr.wikipedia.org/wiki/YouTube", 1),
("https://fr.wikipedia.org/wiki/PayPal", 1),
("https://www.youtube.com/", 1)]

("https://www.lemonde.fr/", 3),
        ("https://www.lefigaro.fr/", 3),
        ("https://www.liberation.fr/", 3),
        ("https://france3-regions.francetvinfo.fr/provence-alpes-cote-d-azur/alpes-maritimes/nice/carnaval-de-nice-2024-cyprien-star-de-youtube-devoile-le-dessin-de-son-char-2900054.html", 3),
        ("https://www.education.gouv.fr/", 3)

# Liste initiale d'URLs provenant de différents domaines
urls = [("https://www.exemple1.com", 2), ("https://www.exemple2.com", 2), ("https://www.exemple3.com", 2)]

crawler = Crawler(urls, depth=2, max_threads=5, request_delay=10)
crawler.start()

# Récupérer et afficher les données collectées
collected_data = crawler.get_data()
for item in collected_data:
    print(f"URL: {item['url']}\nTitle: {item['title']}\nH1 Tags: {item['h1']}\nH2 Tags: {item['h2']}\nMetadata: {item['metadata']}\nText: {item['text']}\n")
"""

"""
    def check_delay(self, url, depth):
        domain = self.get_domain(url)
        next_url, next_depth = url, depth
        wait_to_crawl = False
        last_visit_time = self.last_visited.get(domain)
        with self.lock:
            if last_visit_time and (time.time() - last_visit_time < self.request_delay):
                # Trouver une nouvelle URL si disponible
                found_other_domain = False
                put_back_in_queue = []
                while self.queue:
                    next_url, next_depth = self.queue.pop(0)
                    if self.get_domain(next_url) != domain:
                        self.queue.insert(0, (url, depth))
                        found_other_domain = True
                        break
                    else:
                        put_back_in_queue.append((next_url, next_depth))
                self.queue = put_back_in_queue + self.queue
                if not found_other_domain:
                    # Pas d'autres URLs, attendre
                    wait_to_crawl = True
                
        if wait_to_crawl:
            print(f"J'attends pour crawl : {url}")
            time.sleep(self.request_delay - (time.time() - last_visit_time))
            self.check_delay(url, depth)

        # Mise à jour de la dernière visite et crawl de la page
        with self.lock:
            self.last_visited[domain] = time.time()
        return next_url, next_depth
"""