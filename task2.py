import requests
from bs4 import BeautifulSoup
from collections import Counter
from multiprocessing import Pool
import matplotlib.pyplot as plt
import re

# Функція для завантаження тексту з URL
def fetch_text_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    text = soup.get_text()
    return text

# Функція для розбиття тексту на слова
def tokenize(text):
    text = text.lower()
    words = re.findall(r'\b\w+\b', text)
    return words

# Mapper функція для MapReduce
def mapper(words_chunk):
    word_counts = Counter(words_chunk)
    return word_counts

# Reducer функція для MapReduce
def reducer(counter1, counter2):
    counter1.update(counter2)
    return counter1

# Функція для виконання MapReduce
def mapreduce(text, num_chunks=10):
    words = tokenize(text)
    chunk_size = len(words) // num_chunks
    chunks = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]
    
    with Pool() as pool:
        intermediate_counts = pool.map(mapper, chunks)
    
    total_count = Counter()
    for count in intermediate_counts:
        total_count = reducer(total_count, count)
    
    return total_count

# Функція для візуалізації топ-слова
def visualize_top_words(word_counts, top_n=10):
    most_common_words = word_counts.most_common(top_n)
    words, counts = zip(*most_common_words)
    
    plt.figure(figsize=(10, 6))
    plt.bar(words, counts)
    plt.xlabel('Words')
    plt.ylabel('Frequency')
    plt.title(f'Top {top_n} most frequent words')
    plt.show()

if __name__ == "__main__":
    url = 'https://tsn.ua/ukrayina/ataka-droniv-krimu-ta-vibuhi-v-odesi-golovni-novini-nochi-15-lipnya-2024-roku-2621016.html'
    text = fetch_text_from_url(url)
    word_counts = mapreduce(text)
    visualize_top_words(word_counts)