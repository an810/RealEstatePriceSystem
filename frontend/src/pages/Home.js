import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Container,
  Typography,
  Grid,
  Card,
  CardContent,
  CardActions,
  Button,
  Box,
} from '@mui/material';
import CalculateIcon from '@mui/icons-material/Calculate';
import NotificationsIcon from '@mui/icons-material/Notifications';
import SearchIcon from '@mui/icons-material/Search';
import BarChartIcon from '@mui/icons-material/BarChart';

function Home() {
  return (
    <Container maxWidth="lg" sx={{ mt: 4 }}>
      <Box textAlign="center" mb={6}>
        <Typography variant="h3" component="h1" gutterBottom>
          Welcome to Real Estate Bot
        </Typography>
        <Typography variant="h6" color="text.secondary" paragraph>
          Your AI-powered real estate assistant for Hanoi's property market
        </Typography>
      </Box>

      <Grid container spacing={4}>
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h5" component="h2" gutterBottom>
                Price Prediction
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Get accurate price estimates for properties based on various factors
                including area, location, number of bedrooms, toilets, and legal status.
                Our AI model provides reliable price predictions to help you make informed decisions.
              </Typography>
            </CardContent>
            <CardActions>
              <Button
                size="large"
                component={RouterLink}
                to="/predict"
                startIcon={<CalculateIcon />}
                fullWidth
              >
                Try Prediction
              </Button>
            </CardActions>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h5" component="h2" gutterBottom>
                Property Search
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Search for properties across Hanoi with advanced filters including price range,
                area, number of bedrooms, toilets, districts, and legal status. Find your perfect
                property match with our comprehensive search tool.
              </Typography>
            </CardContent>
            <CardActions>
              <Button
                size="large"
                component={RouterLink}
                to="/search"
                startIcon={<SearchIcon />}
                fullWidth
              >
                Search Properties
              </Button>
            </CardActions>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h5" component="h2" gutterBottom>
                Market Visualizations
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Explore interactive visualizations of Hanoi's real estate market including price
                distributions, area analysis, property type breakdowns, and district-wise insights.
                View properties on an interactive map with detailed information.
              </Typography>
            </CardContent>
            <CardActions>
              <Button
                size="large"
                component={RouterLink}
                to="/visualizations"
                startIcon={<BarChartIcon />}
                fullWidth
              >
                View Visualizations
              </Button>
            </CardActions>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h5" component="h2" gutterBottom>
                Daily Updates
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Subscribe to receive daily real estate news and personalized property
                recommendations based on your preferences. Stay informed about new listings
                and market trends in your areas of interest.
              </Typography>
            </CardContent>
            <CardActions>
              <Button
                size="large"
                component={RouterLink}
                to="/subscribe"
                startIcon={<NotificationsIcon />}
                fullWidth
              >
                Subscribe Now
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>
    </Container>
  );
}

export default Home; 