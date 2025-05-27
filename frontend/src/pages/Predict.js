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
  InputLabel,
  Select,
  MenuItem,
} from '@mui/material';
import axios from 'axios';

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

const validationSchema = yup.object({
  area: yup
    .number()
    .required('Area is required')
    .positive('Area must be positive'),
  number_of_bedrooms: yup
    .number()
    .required('Number of bedrooms is required')
    .integer('Must be a whole number')
    .min(0, 'Must be 0 or greater'),
  number_of_toilets: yup
    .number()
    .required('Number of toilets is required')
    .integer('Must be a whole number')
    .min(0, 'Must be 0 or greater'),
  legal: yup
    .string()
    .required('Legal status is required')
    .oneOf(legalStatuses.map(status => status.value), 'Please select a valid legal status'),
  district: yup
    .string()
    .required('District is required')
    .oneOf(districts, 'Please select a valid district'),
});

function Predict() {
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const formik = useFormik({
    initialValues: {
      area: '',
      number_of_bedrooms: '',
      number_of_toilets: '',
      legal: '',
      district: '',
    },
    validationSchema: validationSchema,
    onSubmit: async (values) => {
      setLoading(true);
      setError(null);
      try {
        const response = await axios.post('http://localhost:8000/predict-price', values);
        setResult(response.data);
      } catch (err) {
        setError(err.response?.data?.detail || 'An error occurred');
      } finally {
        setLoading(false);
      }
    },
  });

  return (
    <Container maxWidth="md" sx={{ mt: 4 }}>
      <Typography variant="h4" component="h1" gutterBottom align="center">
        Property Price Prediction
      </Typography>

      <Paper elevation={3} sx={{ p: 4, mt: 4 }}>
        <form onSubmit={formik.handleSubmit}>
          <Box sx={{ display: 'grid', gap: 2 }}>
            <TextField
              fullWidth
              id="area"
              name="area"
              label="Area (m²)"
              value={formik.values.area}
              onChange={formik.handleChange}
              error={formik.touched.area && Boolean(formik.errors.area)}
              helperText={formik.touched.area && formik.errors.area}
            />

            <TextField
              fullWidth
              id="number_of_bedrooms"
              name="number_of_bedrooms"
              label="Number of Bedrooms"
              value={formik.values.number_of_bedrooms}
              onChange={formik.handleChange}
              error={formik.touched.number_of_bedrooms && Boolean(formik.errors.number_of_bedrooms)}
              helperText={formik.touched.number_of_bedrooms && formik.errors.number_of_bedrooms}
            />

            <TextField
              fullWidth
              id="number_of_toilets"
              name="number_of_toilets"
              label="Number of Toilets"
              value={formik.values.number_of_toilets}
              onChange={formik.handleChange}
              error={formik.touched.number_of_toilets && Boolean(formik.errors.number_of_toilets)}
              helperText={formik.touched.number_of_toilets && formik.errors.number_of_toilets}
            />

            <FormControl fullWidth error={formik.touched.legal && Boolean(formik.errors.legal)}>
              <InputLabel id="legal-label">Legal Status</InputLabel>
              <Select
                labelId="legal-label"
                id="legal"
                name="legal"
                value={formik.values.legal}
                onChange={formik.handleChange}
                label="Legal Status"
              >
                {legalStatuses.map((status) => (
                  <MenuItem key={status.value} value={status.value}>
                    {status.label}
                  </MenuItem>
                ))}
              </Select>
              {formik.touched.legal && formik.errors.legal && (
                <Typography color="error" variant="caption">
                  {formik.errors.legal}
                </Typography>
              )}
            </FormControl>

            <FormControl fullWidth error={formik.touched.district && Boolean(formik.errors.district)}>
              <InputLabel id="district-label">District</InputLabel>
              <Select
                labelId="district-label"
                id="district"
                name="district"
                value={formik.values.district}
                onChange={formik.handleChange}
                label="District"
              >
                {districts.map((district) => (
                  <MenuItem key={district} value={district}>
                    {district}
                  </MenuItem>
                ))}
              </Select>
              {formik.touched.district && formik.errors.district && (
                <Typography color="error" variant="caption">
                  {formik.errors.district}
                </Typography>
              )}
            </FormControl>

            <Button
              color="primary"
              variant="contained"
              fullWidth
              type="submit"
              disabled={loading}
              sx={{ mt: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Predict Price'}
            </Button>
          </Box>
        </form>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {result && (
          <Alert severity="success" sx={{ mt: 2 }}>
            Estimated Price: {result.predicted_price.toLocaleString()} billion VND
          </Alert>
        )}
      </Paper>
    </Container>
  );
}

export default Predict; 