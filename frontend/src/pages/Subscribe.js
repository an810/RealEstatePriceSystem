import React, { useState } from 'react';
import { useFormik } from 'formik';
import * as yup from 'yup';
import {
  Container,
  Typography,
  TextField,
  Button,
  Paper,
  Box,
  Alert,
  CircularProgress,
  FormControl,
  Grid,
  Select,
  MenuItem,
  InputLabel,
} from '@mui/material';
import axios from 'axios';

const validationSchema = yup.object({
  user_id: yup
    .string()
    .required('User ID is required'),
  user_type: yup
    .string()
    .oneOf(['email', 'telegram'], 'User type must be either email or telegram')
    .required('User type is required'),
  price_range: yup.object({
    min_price: yup.number().required('Minimum price is required'),
    max_price: yup.number().required('Maximum price is required'),
  }),
  area_range: yup.object({
    min_area: yup.number().required('Minimum area is required'),
    max_area: yup.number().required('Maximum area is required'),
  }),
  num_bedrooms: yup.number().required('Number of bedrooms is required'),
  num_toilets: yup.number().required('Number of toilets is required'),
  districts: yup.array().min(1, 'Select at least one district'),
  legal_status: yup.string().required('Legal status is required'),
});

const districts = [
  'Ba Đình', 'Ba Vì', 'Cầu Giấy', 'Chương Mỹ', 'Đan Phượng', 'Đông Anh', 'Đống Đa',
  'Gia Lâm', 'Hà Đông', 'Hai Bà Trưng', 'Hoài Đức', 'Hoàn Kiếm', 'Hoàng Mai',
  'Long Biên', 'Mê Linh', 'Mỹ Đức', 'Phú Xuyên', 'Phúc Thọ', 'Quốc Oai', 'Sóc Sơn',
  'Sơn Tây', 'Tây Hồ', 'Thạch Thất', 'Thanh Oai', 'Thanh Trì', 'Thanh Xuân',
  'Thường Tín', 'Từ Liêm', 'Ứng Hòa'
];

const legalStatuses = [
  { value: 'Chưa có sổ', label: 'Chưa có sổ' },
  { value: 'Hợp đồng', label: 'Hợp đồng' },
  { value: 'Sổ đỏ', label: 'Sổ đỏ' },
];

function Subscribe() {
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const formik = useFormik({
    initialValues: {
      user_name: '',
      user_id: '',
      user_type: 'email',
      price_range: {
        min_price: 5,
        max_price: 9,
      },
      area_range: {
        min_area: 30,
        max_area: 60,
      },
      num_bedrooms: 2,
      num_toilets: 2,
      districts: [],
      legal_status: '',
    },
    validationSchema: validationSchema,
    onSubmit: async (values) => {
      setLoading(true);
      setError(null);
      try {
        await axios.post('http://localhost:8000/subscribe', values);
        setSuccess(true);
        formik.resetForm();
      } catch (err) {
        setError(err.response?.data?.detail || 'An error occurred');
      } finally {
        setLoading(false);
      }
    },
  });

  const handleDistrictChange = (event) => {
    formik.setFieldValue('districts', event.target.value);
  };

  return (
    <Container maxWidth="md" sx={{ mt: 4 }}>
      <Typography variant="h4" component="h1" gutterBottom align="center">
        Subscribe to Real Estate Updates
      </Typography>

      <Paper elevation={3} sx={{ p: 4, mt: 4 }}>
        <form onSubmit={formik.handleSubmit}>
          <Box sx={{ display: 'grid', gap: 3 }}>
            <TextField
              fullWidth
              id="user_name"
              name="user_name"
              label="Full Name"
              value={formik.values.user_name}
              onChange={formik.handleChange}
              error={formik.touched.user_name && Boolean(formik.errors.user_name)}
              helperText={formik.touched.user_name && formik.errors.user_name}
            />

            <TextField
              fullWidth
              id="user_id"
              name="user_id"
              label="Email Address"
              value={formik.values.user_id}
              onChange={formik.handleChange}
              error={formik.touched.user_id && Boolean(formik.errors.user_id)}
              helperText={formik.touched.user_id && formik.errors.user_id}
            />

            <Box>
              <Typography gutterBottom>Price Range (billion VND)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    type="number"
                    name="price_range.min_price"
                    label="Min Price"
                    value={formik.values.price_range.min_price}
                    onChange={formik.handleChange}
                    error={formik.touched.price_range?.min_price && Boolean(formik.errors.price_range?.min_price)}
                    helperText={formik.touched.price_range?.min_price && formik.errors.price_range?.min_price}
                  />
                </Grid>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    type="number"
                    name="price_range.max_price"
                    label="Max Price"
                    value={formik.values.price_range.max_price}
                    onChange={formik.handleChange}
                    error={formik.touched.price_range?.max_price && Boolean(formik.errors.price_range?.max_price)}
                    helperText={formik.touched.price_range?.max_price && formik.errors.price_range?.max_price}
                  />
                </Grid>
              </Grid>
            </Box>

            <Box>
              <Typography gutterBottom>Area Range (m²)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    type="number"
                    name="area_range.min_area"
                    label="Min Area"
                    value={formik.values.area_range.min_area}
                    onChange={formik.handleChange}
                    error={formik.touched.area_range?.min_area && Boolean(formik.errors.area_range?.min_area)}
                    helperText={formik.touched.area_range?.min_area && formik.errors.area_range?.min_area}
                  />
                </Grid>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    type="number"
                    name="area_range.max_area"
                    label="Max Area"
                    value={formik.values.area_range.max_area}
                    onChange={formik.handleChange}
                    error={formik.touched.area_range?.max_area && Boolean(formik.errors.area_range?.max_area)}
                    helperText={formik.touched.area_range?.max_area && formik.errors.area_range?.max_area}
                  />
                </Grid>
              </Grid>
            </Box>

            <Grid container spacing={2}>
              <Grid item xs={6}>
                <TextField
                  fullWidth
                  type="number"
                  name="num_bedrooms"
                  label="Number of Bedrooms"
                  value={formik.values.num_bedrooms}
                  onChange={formik.handleChange}
                  error={formik.touched.num_bedrooms && Boolean(formik.errors.num_bedrooms)}
                  helperText={formik.touched.num_bedrooms && formik.errors.num_bedrooms}
                />
              </Grid>
              <Grid item xs={6}>
                <TextField
                  fullWidth
                  type="number"
                  name="num_toilets"
                  label="Number of Toilets"
                  value={formik.values.num_toilets}
                  onChange={formik.handleChange}
                  error={formik.touched.num_toilets && Boolean(formik.errors.num_toilets)}
                  helperText={formik.touched.num_toilets && formik.errors.num_toilets}
                />
              </Grid>
            </Grid>

            <FormControl fullWidth>
              <InputLabel>Districts</InputLabel>
              <Select
                multiple
                value={formik.values.districts}
                onChange={handleDistrictChange}
                label="Districts"
                error={formik.touched.districts && Boolean(formik.errors.districts)}
              >
                {districts.map((district) => (
                  <MenuItem key={district} value={district}>
                    {district}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl fullWidth>
              <InputLabel>Legal Status</InputLabel>
              <Select
                value={formik.values.legal_status}
                onChange={formik.handleChange}
                name="legal_status"
                label="Legal Status"
                error={formik.touched.legal_status && Boolean(formik.errors.legal_status)}
              >
                {legalStatuses.map((status) => (
                  <MenuItem key={status.value} value={status.value}>
                    {status.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <Button
              color="primary"
              variant="contained"
              fullWidth
              type="submit"
              disabled={loading}
              sx={{ mt: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Subscribe'}
            </Button>
          </Box>
        </form>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Alert severity="success" sx={{ mt: 2 }}>
            Successfully subscribed! You will receive updates about properties matching your criteria.
          </Alert>
        )}
      </Paper>
    </Container>
  );
}

export default Subscribe; 