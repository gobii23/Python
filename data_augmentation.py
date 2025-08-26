import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re
import time
import json
import os
import phonenumbers
import numpy as np

# Load environment variables
load_dotenv()

# File paths
INPUT_EXCEL = r"./school_data/excel_input/Top_1000_Teams_Cleaned.xlsx"
OUTPUT_JSON = "./school_data/json_input/schools_data_full_6.json"
OUTPUT_EXCEL = "./school_data/excel_output/Top_1000_Teams_Full_6.xlsx"
os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

class SchoolDataScraper:
    def __init__(self):
        self.api_key = os.getenv("SERPER_API_KEY")
        self.processed_data = []
        self.load_existing_data()

    def load_existing_data(self):
        if os.path.exists(OUTPUT_JSON):
            try:
                with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                    self.processed_data = json.load(f)
                print(f"Loaded {len(self.processed_data)} existing records from JSON")
            except Exception as e:
                print(f"Error loading existing JSON: {e}")
                self.processed_data = []

    def save_to_json(self):
        try:
            # Convert numpy types to Python native types
            data_to_save = []
            for record in self.processed_data:
                safe_record = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in record.items()}
                data_to_save.append(safe_record)
            with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            print(f"Data saved to {OUTPUT_JSON}")
        except Exception as e:
            print(f"Error saving JSON: {e}")

    def is_already_processed(self, school_name, state_ut):
        for record in self.processed_data:
            if (
                record.get("School", "").strip().lower() == school_name.strip().lower()
                and record.get("State/UT", "").strip().lower() == state_ut.strip().lower()
            ):
                return True
        return False

    def search_school_websites(self, school_name, state_ut, retries=5):
        school_name_clean = " ".join(school_name.split())
        state_ut_clean = " ".join(state_ut.split())
        query_text = f"{school_name_clean} {state_ut_clean} official website"

        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        payload = {"q": query_text}
        blacklist = ["indiastudychannel.com"]

        for attempt in range(retries):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=15)
                response.raise_for_status()
                result = response.json()
                websites = []
                if "organic" in result and result["organic"]:
                    for item in result["organic"]:
                        link = item.get("link", "")
                        if link and not any(bad in link for bad in blacklist):
                            if link.startswith(("http://", "https://")):
                                websites.append(link)
                        if len(websites) == 5:
                            break
                if websites:
                    print(f"Top {len(websites)} sites for {school_name_clean}: {websites}")
                    return websites
            except Exception as e:
                print(f"Search attempt {attempt+1} failed for {school_name_clean}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
        print(f"No websites found for {school_name_clean}")
        return []

    def extract_info(self, text, state_ut=""):
        info = {"District": "", "Address": "", "Tel": "", "Email": ""}

        # Extract emails
        emails = re.findall(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", text)
        for e in emails:
            if not any(d in e.lower() for d in ["facebook.com", "twitter.com"]):
                info["Email"] = e
                break

        # Extract phone numbers
        for raw_number in re.split(r"[\/,]", text):
            for match in phonenumbers.PhoneNumberMatcher(raw_number, "IN"):
                info["Tel"] = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                break
            if info["Tel"]:
                break

        # Extract address candidates
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        address_candidates = []
        for line in lines:
            if state_ut and re.search(r"\d{1,4}.*(" + re.escape(state_ut) + r")", line, re.I):
                if not info["Address"]:
                    info["Address"] = line
                address_candidates.append(line)

        # District extraction
        district = ""
        for line in lines:
            if re.search(r"\b(district|dist|dt\.?)\b", line, re.I):
                district = line
                break
        if district:
            district = re.sub(r"(?i)district[:\s-]*|opening of the new|reg|government of|india", "", district)
            parts = [p.strip() for p in district.split(",") if p.strip()]
            if parts:
                info["District"] = parts[-1].title()
        elif address_candidates:
            addr_line = address_candidates[0]
            match = re.search(r"(.+?),?\s*" + re.escape(state_ut), addr_line, re.I)
            if match:
                district_guess = match.group(1).split(",")[-1].strip()
                info["District"] = district_guess.title()

        return info

    def scrape_school_info(self, url, state_ut=""):
        info = {"District": "", "Address": "", "Tel": "", "Email": ""}
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, timeout=15, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract address from location icons
            loc_p = soup.find("p", class_="loc-icon")
            if loc_p:
                info["Address"] = loc_p.get_text(separator=", ", strip=True)

            text = soup.get_text(separator="\n", strip=True)
            info.update(self.extract_info(text, state_ut))

            # Follow About / Contact pages
            for a in soup.find_all("a", href=True):
                page = a["href"]
                if any(k in page.lower() for k in ["contact", "about", "reach-us", "address"]):
                    if not page.startswith("http"):
                        page = requests.compat.urljoin(url, page)
                    try:
                        resp = requests.get(page, headers=headers, timeout=15)
                        resp.raise_for_status()
                        text_page = BeautifulSoup(resp.text, "html.parser").get_text(separator="\n", strip=True)
                        new_info = self.extract_info(text_page, state_ut)
                        for key, val in new_info.items():
                            if not info[key] and val:
                                info[key] = val
                    except:
                        continue
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
        except Exception as e:
            print(f"Failed to scrape {url}: {e}")

        return info

    def process_school(self, row_data, index, total):
        school_name = str(row_data["School"]).replace("\n", " ").strip()
        state_ut = str(row_data["State/UT"]).replace("\n", " ").strip()

        if self.is_already_processed(school_name, state_ut):
            print(f"{index+1}/{total} - {school_name}: Already processed, skipping")
            return

        print(f"{index+1}/{total} - Processing: {school_name}, {state_ut}")

        record = dict(row_data)
        record.update({"Website": "", "District": "", "Address": "", "Tel": "", "Email": ""})

        websites = self.search_school_websites(school_name, state_ut)
        if not websites:
            self.processed_data.append(record)
            self.save_to_json()
            return

        record["Website"] = websites[0]
        merged_info = {"District": "", "Address": "", "Tel": "", "Email": ""}
        for url in websites:
            info = self.scrape_school_info(url, state_ut)
            for key, value in info.items():
                if not merged_info[key] and value:
                    merged_info[key] = value

        record.update(merged_info)
        print(f"Final merged info for {school_name}: Email={bool(record['Email'])}, Tel={bool(record['Tel'])}, District={bool(record['District'])}, Address={bool(record['Address'])}")

        self.processed_data.append(record)
        self.save_to_json()
        time.sleep(1)

    def run(self):
        try:
            print(f"Loading Excel file: {INPUT_EXCEL}")
            df = pd.read_excel(INPUT_EXCEL)
            print(f"Loaded {len(df)} records from Excel")
            start_index = len(self.processed_data)
            if start_index > 0:
                print(f"Resuming from record {start_index+1}/{len(df)}")

            for index, row in df.iterrows():
                if index < start_index:
                    continue
                try:
                    self.process_school(row, index, len(df))
                except KeyboardInterrupt:
                    print("\nProcess interrupted by user. Data saved to JSON.")
                    break
                except Exception as e:
                    print(f"Error processing row {index}: {e}")
                    continue

            self.save_to_excel()
        except Exception as e:
            print(f"Error in main execution: {e}")
        finally:
            print(f"Process completed. Total records processed: {len(self.processed_data)}")

    def save_to_excel(self):
        try:
            if self.processed_data:
                df_output = pd.DataFrame(self.processed_data)
                df_output.to_excel(OUTPUT_EXCEL, index=False)
                print(f"Excel file saved to {OUTPUT_EXCEL}")
            else:
                print("No data to save to Excel")
        except Exception as e:
            print(f"Error saving Excel file: {e}")

def main():
    scraper = SchoolDataScraper()
    scraper.run()

if __name__ == "__main__":
    main()
