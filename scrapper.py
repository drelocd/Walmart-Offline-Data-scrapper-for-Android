from bs4 import BeautifulSoup
import csv
import os
import re
import email
import time
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime

class WalmartOfflineScraper:
    def __init__(self):
        self.reset_tracking()
        self.BRAND_BLACKLIST = {'save with', 'shop', 'buy', 'new'}
        self.SKIP_WORDS = {'with', 'the', 'for', 'of', 'a', 'an'}
        self.STORE_NAME = "ENP Direct Inc"
        self.debug_mode = True
        
        # Load brands from JSON file in working directory
        self.SPECIAL_BRANDS = self.load_brands_from_file('brands.json')
    
    def load_brands_from_file(self, filename):
        """Load brands from JSON file in working directory"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    brands = json.load(f)
                    self.log(f"✅ Successfully loaded brands from: {os.path.abspath(filename)}")
                    return brands
            self.log(f"⚠️ {filename} not found in working directory")
        except Exception as e:
            self.log(f"⚠️ Error loading {filename}: {str(e)}")
        
        # Fallback to default brands
        self.log("⚠️ Using default brands instead")
        return {
            "wallsupply": "Wall!Supply",
            "ah": "A&H",
            "fire flavor": "Fire & Flavor",
            "blackdecker": "Black + Decker",
            "loreal": "L'Oreal"
        }

    def reset_tracking(self):
        self.seen_ids = set()
        self.seen_upcs = set()
        self.all_products = []
        self.current_file_products = 0
    
    def log(self, message):
        if self.debug_mode:
            print(message)

    def parse_mhtml(self, file_path):
        try:
            with open(file_path, 'rb') as f:
                msg = email.message_from_binary_file(f)
            
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or 'utf-8'
                    return payload.decode(charset, errors='ignore')
            return None
        except Exception as e:
            self.log(f"⚠️ MHTML parsing error: {str(e)}")
            return None

    def process_html_file(self, file_path):
        try:
            if not os.path.exists(file_path):
                self.log(f"⚠️ File not found: {file_path}")
                return 0
                
            self.current_file_products = 0
                
            if file_path.lower().endswith('.mhtml'):
                html_content = self.parse_mhtml(file_path)
                if not html_content:
                    return 0
            else:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    html_content = f.read()

            parsers = ["html.parser", "lxml", "html5lib"]
            soup = None
            for parser in parsers:
                try:
                    soup = BeautifulSoup(html_content, parser)
                    break
                except Exception as e:
                    self.log(f"⚠️ Parser {parser} failed: {str(e)}")
                    continue
            
            if not soup:
                self.log(f"⚠️ Could not parse {file_path}")
                return 0
                
            products = self.extract_products(soup)
            new_products = [p for p in products if p['id'] not in self.seen_ids]
            
            self.all_products.extend(new_products)
            self.seen_ids.update(p['id'] for p in new_products)
            self.current_file_products = len(new_products)
            
            self.log(f"✅ Processed {file_path}: {len(products)} products found, {len(new_products)} new")
            return len(new_products)
            
        except Exception as e:
            self.log(f"⚠️ Error processing {file_path}: {str(e)}")
            return 0
    
    def process_all_pages(self, base_filename="ENP Direct, Inc. - Walmart.com", num_pages=25):
        found_files = []
        extensions = ['.html', '.mhtml']
        
        # Look for files in working directory
        for ext in extensions:
            if os.path.exists(f"{base_filename}{ext}"):
                found_files.append(f"{base_filename}{ext}")
                break
        
        for i in range(1, num_pages):
            for ext in extensions:
                filename = f"{base_filename} ({i}){ext}"
                if os.path.exists(filename):
                    found_files.append(filename)
                    break
        
        if not found_files:
            self.log(f"⚠️ No HTML/MHTML files found in: {os.path.abspath(os.getcwd())}")
            self.log(f"Looking for files named like: {base_filename}.mhtml")
            return []
        
        def sort_key(filename):
            match = re.search(r'\((\d+)\)', filename)
            return int(match.group(1)) if match else -1
        
        found_files.sort(key=sort_key)
        
        total_added = 0
        for filename in found_files:
            self.log(f"\n{'='*50}")
            self.log(f"📄 Processing file: {filename}")
            
            file_count = self.process_html_file(filename)
            total_added += file_count
            
            self.log(f"📊 Current file stats: {file_count} new products")
            self.log(f"📊 Total products so far: {len(self.all_products)}")
            
            time.sleep(0.5)
        
        self.log(f"\n{'='*50}")
        self.log(f"🏁 FINAL RESULTS")
        self.log(f"📦 Total files processed: {len(found_files)}")
        self.log(f"🛒 Total unique products: {len(self.all_products)}")
        self.log(f"✨ Total new added: {total_added}")
        
        csv_filename = "Walmart.csv"
        self.save_to_csv(csv_filename)
        return self.all_products
    
    def extract_price(self, price_text):
        if not price_text:
            return None
        clean_text = price_text.replace(",", "").replace("$", "").strip()
        match = re.search(r'(\d+\.\d{2})', clean_text)
        return float(match.group(1)) if match else None
    
    def extract_upc_from_url(self, url):
        if not url:
            return None
        match = re.search(r'/(\d{8,})(?:$|[/?])', url)
        return match.group(1) if match else None
    
    def extract_inventory_count(self, container):
        if not container:
            return None
        text = container.get_text(" ", strip=True).lower()
        patterns = [
            r'only\s*(\d+)\s*left',
            r'(\d+)\s*in\s*stock',
            r'out\s*of\s*stock',
            r'in\s*stock'
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1)) if match.groups() else match.group(0)
        return None
    
    def extract_star_rating(self, container):
        if not container:
            return None
        rating_elem = container.select_one('[aria-label*="out of 5"], [aria-label*="stars"]')
        if rating_elem and 'aria-label' in rating_elem.attrs:
            match = re.search(r'(\d+(?:\.\d+)?)\s*(?:out of 5|stars?)', rating_elem['aria-label'])
            if match:
                return float(match.group(1))
        text_elem = container.find(string=re.compile(r'out of 5|stars?', re.I))
        if text_elem:
            match = re.search(r'(\d+(?:\.\d+)?)\s*(?:out of 5|stars?)', text_elem)
            if match:
                return float(match.group(1))
        return None
    
    def extract_review_count(self, container):
        if not container:
            return 0
        review_elem = (container.select_one('[data-automation-id="product-review-count"]') or
                      container.select_one('.review-count, .f7'))
        if review_elem:
            match = re.search(r'(\d+)', review_elem.get_text().replace(",", ""))
            return int(match.group(1)) if match else 0
        return 0
    
    def extract_product_name(self, container):
        name_elem = (container.select_one('[data-automation-id="product-title"]') or
                    container.select_one('[itemprop="name"]') or
                    container.select_one('.w_Bn, .f6, .title'))
        if not name_elem:
            return None
        name = re.sub(r'^(Save with|Shop|Buy|New)\s*[-:]?\s*', '', name_elem.get_text(strip=True), flags=re.I)
        return name.strip()
    
    def determine_brand(self, container, product_name):
        if not product_name:
            return "Unknown"
            
        clean_product_name = re.sub(r'[^a-z0-9]', '', product_name.lower())
        
        for brand_key, brand_value in self.SPECIAL_BRANDS.items():
            clean_brand_key = re.sub(r'[^a-z0-9]', '', brand_key.lower())
            if clean_brand_key in clean_product_name:
                return brand_value
                
        brand_elem = (container.select_one('[data-automation-id="product-brand"]') or
                     container.select_one('[itemprop="brand"]') or
                     container.select_one('.brand, .f7.mr1'))
        if brand_elem:
            brand_text = brand_elem.get_text(strip=True)
            lower_brand = brand_text.lower()
            if (lower_brand and 
                lower_brand not in self.BRAND_BLACKLIST and
                not re.search(r'^\$?\d+\.?\d*$', brand_text)):
                return brand_text
                
        words = re.findall(r'\b[\w-]+\b', product_name)
        for word in words:
            lower_word = word.lower()
            if (not re.search(r'^\$?\d+\.?\d*$', word) and
                lower_word not in self.SKIP_WORDS and
                lower_word not in self.BRAND_BLACKLIST and
                len(word) > 2):
                return word.title()
                
        return "Unknown"
    
    def extract_products(self, soup):
        products = []
        
        container_selectors = [
            '[data-item-id]',
            '[data-testid="list-view-item"]',
            '.search-result-gridview-item',
            '.mv0'
        ]
        
        containers = []
        for selector in container_selectors:
            found = soup.select(selector)
            if found:
                self.log(f"🔍 Found {len(found)} products with selector: {selector}")
                containers.extend(found)
                break
        
        if not containers:
            self.log("⚠️ No product containers found with any selector")
            return products
        
        for container in containers:
            try:
                product = {
                    'id': (container.get('data-item-id') or 
                          container.get('id') or 
                          container.get('data-product-id', '')).strip(),
                    'name': self.extract_product_name(container),
                    'brand': None,
                    'price': None,
                    'url': None,
                    'upc': None,
                    'rating': None,
                    'reviews': None,
                    'image_url': None,
                    'inventory_count': None,
                    'is_walmart_seller': False,
                    'seller_name': self.STORE_NAME,
                    'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                if not product['id']:
                    self.log("⚠️ Skipping product - Missing ID")
                    continue
                    
                if product['id'] in self.seen_ids:
                    self.log(f"⏩ Skipping duplicate product ID: {product['id']}")
                    continue
                    
                price_elem = container.select_one('[data-automation-id="product-price"]') or \
                             container.select_one('[itemprop="price"]') or \
                             container.select_one('.price, .b.black')
                if price_elem:
                    product['price'] = self.extract_price(price_elem.get_text(strip=True))
                    
                link = container.select_one('a[href]')
                if link:
                    product['url'] = urljoin('https://www.walmart.com', link['href'])
                    product['upc'] = self.extract_upc_from_url(product['url'])
                
                product['brand'] = self.determine_brand(container, product['name'])
                if product['brand'] == "Unknown":
                    self.log(f"⚠️ Skipping product - Unknown brand: {product['name']}")
                    continue
                    
                product['rating'] = self.extract_star_rating(container)
                product['reviews'] = self.extract_review_count(container)
                
                img = container.select_one('img[src], img[data-image-src]')
                if img:
                    product['image_url'] = img.get('src') or img.get('data-image-src')
                
                product['inventory_count'] = self.extract_inventory_count(container)
                
                products.append(product)
                
            except Exception as e:
                self.log(f"⚠️ Error extracting product: {str(e)}")
                continue
                
        return products
    
    def save_to_csv(self, filename):
        if not self.all_products:
            self.log("No products to save")
            return
            
        fieldnames = [
            'id', 'brand', 'name', 'price', 'rating', 'reviews',
            'inventory_count', 'is_walmart_seller', 'seller_name',
            'upc', 'url', 'image_url', 'scrape_date'
        ]
        
        full_path = os.path.abspath(filename)
        try:
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for product in self.all_products:
                    cleaned_product = {k: v if v is not None else '' for k, v in product.items()}
                    writer.writerow(cleaned_product)
            self.log(f"✅ Saved {len(self.all_products)} products to:")
            self.log(f"📂 {full_path}")
        except Exception as e:
            self.log(f"⚠️ Error saving CSV: {str(e)}")
            self.log(f"Attempted path: {full_path}")
    
    def show_sample_output(self, count=5):
        if not self.all_products:
            self.log("No products to display")
            return
            
        self.log(f"\n{'='*50}")
        self.log(f"TOTAL PRODUCTS: {len(self.all_products)}")
        self.log(f"SAMPLE PRODUCTS (FIRST {count}):")
        self.log(f"{'='*50}")
        
        for idx, product in enumerate(self.all_products[:count], 1):
            self.log(f"\n#{idx} {product.get('brand', 'Unknown')} - {product.get('name', '')[:60]}...")
            self.log(f"Price: ${product.get('price', 'N/A')}")
            self.log(f"Rating: {product.get('rating', 'N/A')} ({product.get('reviews', 0)} reviews)")
            self.log(f"Stock: {product.get('inventory_count', 'Unknown')}")
            self.log(f"UPC: {product.get('upc', 'None')}")
            self.log(f"URL: {product.get('url', '')[:70]}...")

if __name__ == "__main__":
    scraper = WalmartOfflineScraper()
    scraper.process_all_pages()
    scraper.show_sample_output()