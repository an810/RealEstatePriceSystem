import React, { useEffect, useState } from 'react';
import Plot from 'react-plotly.js';
import { Box, Typography, CircularProgress, Grid, Paper, Alert } from '@mui/material';

const Visualizations = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Color mapping exactly like backend
    const PROPERTY_TYPE_COLORS = {
        "Chung cư": "#1f77b4",  // Blue
        "Biệt thự": "#ff7f0e",  // Orange
        "Nhà riêng": "#2ca02c", // Green
        "Đất": "#d62728",       // Red
        "Khác": "#9467bd"       // Purple
    };

  useEffect(() => {
    const fetchData = async () => {
      try {
        console.log('Fetching visualization data...');
        const response = await fetch('http://localhost:8000/api/visualizations');
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || 'Failed to fetch visualization data');
        }
        const visualizationData = await response.json();
        console.log('Received data:', visualizationData);
        setData(visualizationData);
      } catch (error) {
        console.error('Error fetching visualization data:', error);
        setError(error.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="error">
          Error loading visualizations: {error}
        </Alert>
      </Box>
    );
  }

  if (!data) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="warning">
          No visualization data available
        </Alert>
      </Box>
    );
  }

  const renderPlot = (plotData, height = 400) => {
    if (!plotData) {
      console.warn('Missing plot data');
      return null;
    }

    console.log('Rendering plot:', plotData.type);

    const layout = {
      height,
      margin: { t: 40, r: 20, l: 60, b: 40 },
      title: {
        text: plotData.title,
        font: { size: 16 }
      },
      xaxis: { 
        title: plotData.xaxis_title,
        automargin: true
      },
      yaxis: { 
        title: plotData.yaxis_title,
        automargin: true
      },
      showlegend: true,
      autosize: true,
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
    };

    const config = {
      responsive: true,
      displayModeBar: true,
      displaylogo: false,
      modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    };

    switch (plotData.type) {
      case 'histogram':
        return (
          <Plot
            data={[
              {
                type: 'histogram',
                x: plotData.x,
                nbinsx: plotData.nbins,
                marker: {
                  color: plotData.color
                }
              },
            ]}
            layout={layout}
            config={config}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      case 'pie':
        return (
          <Plot
            data={[
              {
                type: 'pie',
                labels: plotData.labels,
                values: plotData.values,
                marker: {
                  colors: plotData.colors
                },
                textinfo: 'label+percent',
                textposition: 'outside',
                automargin: true
              },
            ]}
            layout={{
              ...layout,
              height: height * 1.2,
              margin: { t: 40, r: 20, l: 20, b: 40 }
            }}
            config={config}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      case 'bar':
        return (
          <Plot
            data={[
              {
                type: 'bar',
                x: plotData.x,
                y: plotData.y,
                marker: {
                  color: plotData.color
                }
              },
            ]}
            layout={{
              ...layout,
              xaxis: { 
                ...layout.xaxis,
                tickangle: -45,
                automargin: true
              }
            }}
            config={config}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      case "scatter": {
        const propertyTypes = [...new Set(plotData.hover_data.property_type)];

        // Prepare traces per property type
        const traces = propertyTypes.map((ptype) => {
          const indexes = plotData.hover_data.property_type
            .map((pt, i) => (pt === ptype ? i : -1))
            .filter((i) => i !== -1);

          return {
            type: "scatter",
            mode: "markers",
            x: indexes.map((i) => plotData.x[i]),
            y: indexes.map((i) => plotData.y[i]),
            marker: {
              size: 8,
              opacity: 0.7,
              color: PROPERTY_TYPE_COLORS[ptype] || "gray",
            },
            name: ptype,
            text: indexes.map((i) => plotData.hover_data.title[i]),
            customdata: indexes.map((i) => [
              plotData.hover_data.district[i],
              plotData.hover_data.property_type[i],
            ]),
            hovertemplate:
              "<b>%{text}</b><br>" +
              "Area: %{x:.0f} m²<br>" +
              "Price: %{y:.1f} tỷ VND<br>" +
              "District: %{customdata[0]}<br>" +
              "Type: %{customdata[1]}<extra></extra>",
          };
        });

        return (
          <Plot
            data={traces}
            layout={{ ...layout }}
            config={config}
            style={{ width: "100%", height: "100%" }}
            useResizeHandler={true}
          />
        );
      }

      case "scattermapbox": {
        console.log('Scattermapbox plotData:', plotData);
        const propertyTypes = [...new Set(plotData.hover_data.property_type)];

        // Scatter markers per property type
        const scatterTraces = propertyTypes.map((ptype) => {
          const indexes = plotData.hover_data.property_type
            .map((pt, i) => (pt === ptype ? i : -1))
            .filter((i) => i !== -1);

          return {
            type: "scattermapbox",
            lat: indexes.map((i) => plotData.lat[i]),
            lon: indexes.map((i) => plotData.lon[i]),
            mode: "markers",
            marker: {
              size: indexes.map((i) => plotData.size[i]),
              color: PROPERTY_TYPE_COLORS[ptype] || "gray",
              opacity: 0.7,
            },
            text: indexes.map((i) => plotData.hover_name[i]),
            name: ptype,
            customdata: indexes.map((i) => [
              plotData.hover_data.district[i],
              plotData.hover_data.price[i],
              plotData.hover_data.area[i],
              plotData.hover_data.property_type[i],
            ]),
            hovertemplate:
              "<b>%{text}</b><br>" +
              "District: %{customdata[0]}<br>" +
              "Price: %{customdata[1]:.1f} tỷ VND<br>" +
              "Area: %{customdata[2]:.0f} m²<br>" +
              "Type: %{customdata[3]}<extra></extra>",
          };
        });

        return (
          <Plot
            data={scatterTraces}
            layout={{
              ...layout,
              mapbox: {
                style: "open-street-map",
                center: { lat: 21.0285, lon: 105.8542 },
                zoom: plotData.zoom,
              },
              margin: { r: 0, t: 40, l: 0, b: 0 },
            }}
            config={config}
            style={{ width: "100%", height: "100%" }}
            useResizeHandler={true}
          />
        );
      }

      case 'choroplethmapbox':
        console.log('Choroplethmapbox plotData:', plotData);
        return (
          <Plot
            data={[
              {
                type: 'choroplethmapbox',
                geojson: plotData.geojson,
                locations: plotData.locations,
                z: plotData.z,
                colorscale: plotData.colorscale,
                marker: plotData.marker,
                colorbar: plotData.colorbar,
              },
              {
                type: 'scattermapbox',
                lat: plotData.district_labels.lat,
                lon: plotData.district_labels.lon,
                mode: 'text',
                text: plotData.district_labels.text,
                textfont: { size: 11, color: 'black' },
                hoverinfo: 'skip',
              },
            ]}
            layout={{
              ...layout,
              mapbox: {
                style: 'open-street-map',
                center: plotData.center,
                zoom: plotData.zoom,
              },
              margin: { r: 0, t: 40, l: 0, b: 0 },
            }}
            config={config}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      default:
        console.warn('Unknown plot type:', plotData.type);
        return null;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Real Estate Data Visualizations
      </Typography>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 400, overflow: 'hidden' }}>
            {renderPlot(data?.price_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 400, overflow: 'hidden' }}>
            {renderPlot(data?.area_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 400, overflow: 'hidden' }}>
            {renderPlot(data?.property_type_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 400, overflow: 'hidden' }}>
            {renderPlot(data?.district_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 500, overflow: 'hidden' }}>
            {renderPlot(data?.price_area_scatter, 500)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 600, overflow: 'hidden' }}>
            {renderPlot(data?.listings_map, 600)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 600, overflow: 'hidden' }}>
            {renderPlot(data?.choropleth_map, 600)}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Visualizations;