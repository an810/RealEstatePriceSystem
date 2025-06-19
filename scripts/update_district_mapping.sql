-- Update district_mapping table to include Bắc Từ Liêm and Nam Từ Liêm
-- First, delete the old Từ Liêm entry
DELETE FROM district_mapping WHERE district = 'Từ Liêm';

-- Insert the new district mappings
INSERT INTO district_mapping (district_id, district) VALUES
    (11, 'Nam Từ Liêm'),
    (13, 'Bắc Từ Liêm')
ON CONFLICT (district_id) DO UPDATE SET 
    district = EXCLUDED.district,
    updated_at = CURRENT_TIMESTAMP;

-- Update existing district IDs to match the mapping file
UPDATE district_mapping SET district_id = 28, updated_at = CURRENT_TIMESTAMP WHERE district = 'Hoài Đức';
UPDATE district_mapping SET district_id = 29, updated_at = CURRENT_TIMESTAMP WHERE district = 'Hoàn Kiếm';
UPDATE district_mapping SET district_id = 30, updated_at = CURRENT_TIMESTAMP WHERE district = 'Hoàng Mai';

-- Verify the changes
SELECT district_id, district FROM district_mapping ORDER BY district_id; 