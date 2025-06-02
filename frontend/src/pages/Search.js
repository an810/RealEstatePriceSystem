import React, { useState } from 'react';
import {
  Box,
  Container,
  Typography,
  TextField,
  Button,
  Grid,
  Card,
  CardContent,
  FormControl,
  InputLabel,
  Select, 
  MenuItem,
  Chip,
  Paper,
  CircularProgress,
  Alert,
} from '@mui/material';
import axios from 'axios';

const DISTRICTS = [
  'Ba Đình', 'Ba Vì', 'Cầu Giấy', 'Chương Mỹ', 'Đan Phượng', 'Đông Anh', 
  'Đống Đa', 'Gia Lâm', 'Hà Đông', 'Hai Bà Trưng', 'Hoài Đức', 'Hoàn Kiếm', 
  'Hoàng Mai', 'Long Biên', 'Mê Linh', 'Quốc Oai', 'Sóc Sơn', 'Sơn Tây', 
  'Tây Hồ', 'Thanh Oai', 'Thanh Trì', 'Thanh Xuân', 'Thạch Thất', 'Thường Tín', 
  'Từ Liêm'
];

const LEGAL_STATUSES = ['Chưa có sổ', 'Hợp đồng', 'Sổ đỏ'];

const PROPERTY_TYPES = ['Chung cư', 'Biệt thự', 'Nhà riêng', 'Đất'];

const Search = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [resultsByDistrict, setResultsByDistrict] = useState({});
  const [formData, setFormData] = useState({
    price_range: {
      min_price: '',
      max_price: '',
    },
    area_range: {
      min_area: '',
      max_area: '',
    },
    num_bedrooms: '',
    num_toilets: '',
    districts: [],
    legal_status: '',
    property_type: '',
  });

  const handleInputChange = (field, value) => {
    if (field.includes('.')) {
      const [parent, child] = field.split('.');
      setFormData(prev => ({
        ...prev,
        [parent]: {
          ...prev[parent],
          [child]: value,
        },
      }));
    } else {
      setFormData(prev => ({
        ...prev,
        [field]: value,
      }));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setResultsByDistrict({});

    try {
      console.log(formData);
      const response = await axios.post('http://localhost:8000/search', formData);
      setResultsByDistrict(response.data.results_by_district);
    } catch (err) {
      setError(err.response?.data?.detail || 'An error occurred while searching');
    } finally {
      setLoading(false);
    }
  };

  const formatPrice = (price) => {
    return `${price.toFixed(1)} billion VND`;
  };

  const handleDeleteDistrict = (districtToDelete) => {
    setFormData(prev => ({
      ...prev,
      districts: prev.districts.filter(district => district !== districtToDelete)
    }));
  };

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Typography variant="h4" component="h1" gutterBottom>
        Property Search
      </Typography>

      <Paper elevation={3} sx={{ p: 3, mb: 4 }}>
        <form onSubmit={handleSubmit}>
          <Grid container spacing={3}>
            {/* Price Range */}
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Minimum Price (billion VND)"
                type="number"
                value={formData.price_range.min_price}
                onChange={(e) => handleInputChange('price_range.min_price', e.target.value)}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Maximum Price (billion VND)"
                type="number"
                value={formData.price_range.max_price}
                onChange={(e) => handleInputChange('price_range.max_price', e.target.value)}
                required
              />
            </Grid>

            {/* Area Range */}
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Minimum Area (m²)"
                type="number"
                value={formData.area_range.min_area}
                onChange={(e) => handleInputChange('area_range.min_area', e.target.value)}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Maximum Area (m²)"
                type="number"
                value={formData.area_range.max_area}
                onChange={(e) => handleInputChange('area_range.max_area', e.target.value)}
                required
              />
            </Grid>

            {/* Number of Bedrooms and Toilets */}
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Number of Bedrooms"
                type="number"
                value={formData.num_bedrooms}
                onChange={(e) => handleInputChange('num_bedrooms', e.target.value)}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Number of Toilets"
                type="number"
                value={formData.num_toilets}
                onChange={(e) => handleInputChange('num_toilets', e.target.value)}
                required
              />
            </Grid>

            {/* Districts */}
            <Grid item xs={12}>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <Typography variant="subtitle1">Districts</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {DISTRICTS.map((district) => (
                    <Chip
                      key={district}
                      label={district}
                      onClick={() => {
                        if (!formData.districts.includes(district)) {
                          handleInputChange('districts', [...formData.districts, district]);
                        }
                      }}
                      onDelete={formData.districts.includes(district) ? () => handleDeleteDistrict(district) : undefined}
                      color={formData.districts.includes(district) ? "primary" : "default"}
                      sx={{
                        '& .MuiChip-deleteIcon': {
                          color: 'rgba(0, 0, 0, 0.54)',
                          '&:hover': {
                            color: 'rgba(0, 0, 0, 0.87)',
                          },
                        },
                      }}
                    />
                  ))}
                </Box>
              </Box>
            </Grid>

            {/* Legal Status */}
            <Grid item xs={12} md={6}>
              <FormControl fullWidth variant="outlined">
                <InputLabel id="legal-status-label" sx={{ backgroundColor: 'white', px: 1 }}>Legal Status</InputLabel>
                <Select
                  labelId="legal-status-label"
                  value={formData.legal_status}
                  onChange={(e) => handleInputChange('legal_status', e.target.value)}
                  required
                >
                  {LEGAL_STATUSES.map((status) => (
                    <MenuItem key={status} value={status}>
                      {status}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>

            {/* Property Type */}
            <Grid item xs={12} md={6}>
              <FormControl fullWidth variant="outlined">
                <InputLabel id="property-type-label" sx={{ backgroundColor: 'white', px: 1 }}>Property Type</InputLabel>
                <Select
                  labelId="property-type-label"
                  value={formData.property_type}
                  onChange={(e) => handleInputChange('property_type', e.target.value)}
                  required
                >
                  {PROPERTY_TYPES.map((type) => (
                    <MenuItem key={type} value={type}>
                      {type}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>

            {/* Submit Button */}
            <Grid item xs={12}>
              <Button
                type="submit"
                variant="contained"
                color="primary"
                fullWidth
                disabled={loading}
                sx={{ mt: 2 }}
              >
                {loading ? <CircularProgress size={24} /> : 'Search Properties'}
              </Button>
            </Grid>
          </Grid>
        </form>
      </Paper>

      {/* Error Message */}
      {error && (
        <Alert severity="error" sx={{ mb: 4 }}>
          {error}
        </Alert>
      )}

      {/* Results */}
      {Object.keys(resultsByDistrict).length > 0 && (
        <Box>
          <Typography variant="h5" gutterBottom>
            Search Results
          </Typography>
          {Object.entries(resultsByDistrict).map(([district, properties]) => (
            <Box key={district} sx={{ mb: 4 }}>
              <Typography variant="h6" gutterBottom sx={{ mt: 2, color: 'primary.main' }}>
                {district}
              </Typography>
              {properties.length === 0 ? (
                <Typography color="textSecondary">
                  No matching properties found in this district.
                </Typography>
              ) : (
                <Grid container spacing={3}>
                  {properties.map((property) => (
                    <Grid item xs={12} md={6} lg={4} key={property.id}>
                      <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
                        <CardContent sx={{ flexGrow: 1 }}>
                          <Typography variant="h6" gutterBottom>
                            {property.title}
                          </Typography>
                          <Typography color="textSecondary" gutterBottom>
                            Price: {formatPrice(property.price)}
                          </Typography>
                          <Typography color="textSecondary" gutterBottom>
                            Area: {property.area}m²
                          </Typography>
                          <Typography color="textSecondary" gutterBottom>
                            Bedrooms: {property.number_of_bedrooms} | Toilets: {property.number_of_toilets}
                          </Typography>
                          <Typography color="textSecondary" gutterBottom>
                            Legal Status: {property.legal_status}
                          </Typography>
                          <Typography color="textSecondary" gutterBottom>
                            Property Type: {property.property_type}
                          </Typography>
                          {property.url && (
                            <Typography color="primary" gutterBottom>
                              <a 
                                href={property.url} 
                                target="_blank" 
                                rel="noopener noreferrer"
                                style={{ textDecoration: 'none' }}
                              >
                                View on {property.source}
                              </a>
                            </Typography>
                          )}
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              )}
            </Box>
          ))}
        </Box>
      )}
    </Container>
  );
};

export default Search; 