INSERT INTO property_types (id, name, slug, listing_type, attributes_schema) VALUES
-- BÁN (13 loại hình)
(
    gen_random_uuid(),
    'Căn hộ chung cư',
    'can-ho-chung-cu',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "huong_nha", "huong_ban_cong", "noi_that", "tang_so", "block_thap", "ma_can"]}'::jsonb
),
(
    gen_random_uuid(),
    'Chung cư mini, căn hộ dịch vụ',
    'chung-cu-mini-can-ho-dich-vu',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "huong_nha", "huong_ban_cong", "noi_that", "tang_so"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà ở',
    'nha-o',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà biệt thự độc lập',
    'nha-biet-thu-doc-lap',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà biệt thự liền kề',
    'nha-biet-thu-lien-ke',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Shophouse',
    'shophouse',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Penhouse',
    'penhouse',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất thổ cư',
    'dat-tho-cu',
    'sale',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất nền dự án',
    'dat-nen-du-an',
    'sale',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất nông nghiệp',
    'dat-nong-nghiep',
    'sale',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Trang trại, khu nghỉ dưỡng',
    'trang-trai-khu-nghi-duong',
    'sale',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Kho, nhà xưởng',
    'kho-nha-xuong',
    'sale',
    '{"fields": ["mat_tien", "chieu_cao", "so_phong_tam", "huong_nha", "duong_vao"]}'::jsonb
),
(
    gen_random_uuid(),
    'Loại BĐS khác',
    'loai-bds-khac',
    'sale',
    '{"fields": []}'::jsonb
),

-- CHO THUÊ (13 loại hình)
(
    gen_random_uuid(),
    'Căn hộ chung cư',
    'can-ho-chung-cu',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "huong_nha", "huong_ban_cong", "noi_that", "tang_so", "block_thap", "ma_can"]}'::jsonb
),
(
    gen_random_uuid(),
    'Chung cư mini, căn hộ dịch vụ',
    'chung-cu-mini-can-ho-dich-vu',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "huong_nha", "huong_ban_cong", "noi_that", "tang_so"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà ở',
    'nha-o',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà biệt thự độc lập',
    'nha-biet-thu-doc-lap',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Nhà biệt thự liền kề',
    'nha-biet-thu-lien-ke',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Shophouse',
    'shophouse',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Penhouse',
    'penhouse',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất thổ cư',
    'dat-tho-cu',
    'rent',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất nền dự án',
    'dat-nen-du-an',
    'rent',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Đất nông nghiệp',
    'dat-nong-nghiep',
    'rent',
    '{"fields": ["duong_vao", "huong_nha", "mat_tien"]}'::jsonb
),
(
    gen_random_uuid(),
    'Trang trại, khu nghỉ dưỡng',
    'trang-trai-khu-nghi-duong',
    'rent',
    '{"fields": ["so_phong_ngu", "so_phong_tam", "so_tang", "mat_tien", "duong_vao", "huong_nha", "huong_ban_cong", "noi_that"]}'::jsonb
),
(
    gen_random_uuid(),
    'Kho, nhà xưởng',
    'kho-nha-xuong',
    'rent',
    '{"fields": ["mat_tien", "chieu_cao", "so_phong_tam", "huong_nha", "duong_vao"]}'::jsonb
),
(
    gen_random_uuid(),
    'Loại BĐS khác',
    'loai-bds-khac',
    'rent',
    '{"fields": []}'::jsonb
),