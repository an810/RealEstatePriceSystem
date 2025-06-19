# 1. LINK COLLECTION PHASE
class LinkCollector:
    def collect_links():
        # For each source (BatDongSan and Nhatot)
        for source in ['batdongsan', 'nhatot']:
            # Initialize scraper with source-specific parameters
            scraper = SourceScraper(
                output_file=f"data/crawled/{source}_links.txt",
                error_file=f"data/crawled/{source}_error_links.txt"
            )
            
            # Load existing links from file
            scraper.load_existing_links()
            
            # Scrape all pages and collect links
            scraper.scrape_all_pages()
            
            # Load scraped links from database
            scraped_links = scraper.load_scraped_links_from_db()
            
            # Filter out already scraped links
            unscraped_links = all_links - scraped_links
            
            # Save only unscraped links to file
            scraper.save_links(unscraped_links)

# 2. DATA SCRAPING PHASE
class DataScraper:
    def scrape_data():
        # For each source
        for source in ['batdongsan', 'nhatot']:
            # Read unscraped links from file
            links = read_links(f"data/crawled/{source}_links.txt")
            
            # Initialize scraper with source-specific parameters
            scraper = SourceScraper()
            
            # Scrape each link and save to TSV files
            for link in links:
                data = scraper.scrape_one_url(link)
                if data:
                    save_to_tsv(data, f"data/crawled/{source}.tsv")
                    save_to_tsv(url_data, f"data/crawled/{source}_url.tsv")

# 3. DATA PROCESSING PHASE
class DataProcessor:
    def process_data():
        # For each source
        for source in ['batdongsan', 'nhatot']:
            # Read raw data
            df = read_tsv(f"data/crawled/{source}.tsv")
            
            # Clean and transform data
            df = clean_data(df)
            df = transform_data(df)
            
            # Save processed data
            save_to_tsv(df, f"data/output/processed_{source}.tsv")

# 4. DATA MERGING AND CLEANING PHASE
class DataCleaner:
    def clean_data():
        # Read processed data from both sources
        df1 = read_tsv("data/output/processed_batdongsan.tsv")
        df2 = read_tsv("data/output/processed_nhatot.tsv")
        
        # Merge data
        merged_df = pd.concat([df1, df2])
        
        # Remove unnecessary columns
        merged_df = remove_columns(merged_df)
        
        # Filter invalid data
        merged_df = filter_invalid_data(merged_df)
        
        # Remove outliers
        merged_df = remove_outliers(merged_df)
        
        # Create district mapping
        district_mapping = create_district_mapping(merged_df)
        
        # Infer property types
        merged_df = add_property_types(merged_df)
        
        # Save cleaned data
        save_to_tsv(merged_df, "data/cleaned/visualization_data.tsv")
        save_to_tsv(merged_df, "data/cleaned/processed_data.tsv")

# 5. DATABASE SAVING PHASE
class DatabaseSaver:
    def save_to_database():
        # Create database tables and indexes
        create_tables()
        create_indexes()
        
        # Load cleaned data
        df = read_tsv("data/cleaned/visualization_data.tsv")
        
        # Load district mapping
        district_mapping = load_district_mapping()
        
        # Convert district names to IDs
        df = convert_district_names(df, district_mapping)
        
        # Load URL mappings
        url_df = load_url_mappings()
        
        # Merge with URL data
        df = merge_with_url_data(df, url_df)
        
        # Save to database with upsert
        save_to_database(df)

# MAIN PIPELINE
def run_pipeline():
    # 1. Collect links
    link_collector = LinkCollector()
    link_collector.collect_links()
    
    # 2. Scrape data
    data_scraper = DataScraper()
    data_scraper.scrape_data()
    
    # 3. Process data
    data_processor = DataProcessor()
    data_processor.process_data()
    
    # 4. Clean and merge data
    data_cleaner = DataCleaner()
    data_cleaner.clean_data()
    
    # 5. Save to database
    db_saver = DatabaseSaver()
    db_saver.save_to_database()