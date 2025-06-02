import React, { useEffect, useState, useMemo, useCallback } from 'react';
import Plot from 'react-plotly.js';
import { Box, Typography, CircularProgress, Grid, Paper, Alert } from '@mui/material';

// Memoize the color mapping
const PROPERTY_TYPE_COLORS = {
    "Chung cư": "#1f77b4",  // Blue
    "Biệt thự": "#ff7f0e",  // Orange
    "Nhà riêng": "#2ca02c", // Green
    "Đất": "#d62728",       // Red
    "Khác": "#9467bd"       // Purple
};

// Memoize the default layout configuration
const defaultLayout = {
  margin: { t: 40, r: 20, l: 60, b: 40 },
  showlegend: true,
  autosize: true,
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
};

// Memoize the default config
const defaultConfig = {
  responsive: true,
  displayModeBar: true,
  displaylogo: false,
  modeBarButtonsToRemove: ['lasso2d', 'select2d'],
};

const Visualizations = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/visualizations');
        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.detail || 'Failed to fetch visualization data');
        }
        const visualizationData = await response.json();
        setData(visualizationData);
        setTimeout(() => {
          setMapLoaded(true);
        }, 3000);
      } catch (error) {
        setError(error.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // Memoize the renderPlot function
  const renderPlot = useCallback((plotData, height = 400) => {
    if (!plotData) return null;

    const layout = {
      ...defaultLayout,
      height,
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
    };

    switch (plotData.type) {
      case 'histogram': {
        const totalCount = plotData.x.length;
        const minPrice = Math.min(...plotData.x);
        const maxPrice = Math.max(...plotData.x);
        const binSize = (maxPrice - minPrice) / plotData.nbins;
        
        // Determine the type of histogram and set appropriate hover template
        let hoverTemplate;
        if (plotData.title.includes("Distribution of Property Prices")) {
          hoverTemplate = 
            "Price Range: %{customdata[0]:.2f} - %{customdata[1]:.2f} billion VND<br>" +
            "Number of Properties: %{y}<br>" +
            "Percentage: %{customdata[2]:.1%}<extra></extra>";
        } else if (plotData.title.includes("Distribution of Property Areas")) {
          hoverTemplate = 
            "Area Range: %{customdata[0]:.0f} - %{customdata[1]:.0f} m²<br>" +
            "Number of Properties: %{y}<br>" +
            "Percentage: %{customdata[2]:.1%}<extra></extra>";
        } else if (plotData.title.includes("Price per Area")) {
          hoverTemplate = 
            "Price per Area Range: %{customdata[0]:.2f} - %{customdata[1]:.2f} million VND/m²<br>" +
            "Number of Properties: %{y}<br>" +
            "Percentage: %{customdata[2]:.1%}<extra></extra>";
        } else {
          hoverTemplate = 
            "Range: %{customdata[0]:.2f} - %{customdata[1]:.2f}<br>" +
            "Count: %{y}<br>" +
            "Percentage: %{customdata[2]:.1%}<extra></extra>";
        }
        
        return (
          <Plot
            data={[{
              type: 'histogram',
              x: plotData.x,
              nbinsx: plotData.nbins,
              marker: { color: plotData.color },
              name: 'on/off',
              hovertemplate: hoverTemplate,
              customdata: plotData.x.map((_, i) => {
                const binStart = minPrice + (i * binSize);
                const binEnd = minPrice + ((i + 1) * binSize);
                const binCount = plotData.x.filter(x => x >= binStart && x < binEnd).length;
                return [binStart, binEnd, binCount / totalCount];
              })
            }]}
            layout={{
              ...layout,
              xaxis: { 
                ...layout.xaxis,
                title: { 
                  text: plotData.xaxis_title,
                  font: { size: 14 }
                },
                tickformat: plotData.title.includes("Area Distribution") ? '.0f' : '.2f',
                showgrid: true,
                gridcolor: '#E5E5E5'
              },
              yaxis: { 
                ...layout.yaxis,
                title: { 
                  text: plotData.yaxis_title,
                  font: { size: 14 }
                },
                showgrid: true,
                gridcolor: '#E5E5E5'
              },
              bargap: 0.1,
              bargroupgap: 0.1
            }}
            config={defaultConfig}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );
      }

      case 'pie':
        const totalPie = plotData.values.reduce((a, b) => a + b, 0);
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
                textinfo: plotData.title === "Property Type Distribution" ? 'label+percent' : 'percent',
                textposition: 'outside',
                automargin: true,
                texttemplate: plotData.title === "Property Type Distribution" ? '%{label}<br>%{percent:.1%}' : '%{percent:.1%}',
                hovertemplate: 
                  "<b>%{label}</b><br>" +
                  "Count: %{value}<br>" +
                  "Percentage: %{customdata:.1%}<extra></extra>",
                customdata: plotData.values.map(v => v / totalPie),
                domain: {
                  x: [0.1, 0.7],
                  y: [0.1, 0.9]
                }
              },
            ]}
            layout={{
              ...layout,
              height: height * 1,
              margin: { t: 60, r: 20, l: 20, b: 40 },
              showlegend: true,
              legend: {
                orientation: 'v',
                yanchor: 'middle',
                y: 0.5,
                xanchor: 'left',
                x: 0.75,
                font: {
                  size: 12
                }
              }
            }}
            config={defaultConfig}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      case 'bar':
        const totalListings = plotData.y.reduce((a, b) => a + b, 0);
        return (
          <Plot
            data={[
              {
                type: 'bar',
                x: plotData.x,
                y: plotData.y,
                marker: {
                  color: plotData.color
                },
                name: 'on/off',
                hovertemplate: 
                  "<b>%{x}</b><br>" +
                  "Number of Listings: %{y}<br>" +
                  "Percentage: %{customdata:.1%}<extra></extra>",
                customdata: plotData.y.map(y => y / totalListings)
              },
            ]}
            layout={{
              ...layout,
              xaxis: { 
                ...layout.xaxis,
                title: {
                  text: plotData.xaxis_title,
                  font: { size: 14 },
                  standoff: 20
                },
                tickangle: -45,
                automargin: true,
                showgrid: true,
                gridcolor: '#E5E5E5'
              },
              yaxis: {
                ...layout.yaxis,
                title: {
                  text: plotData.yaxis_title,
                  font: { size: 14 },
                  standoff: 20
                },
                showgrid: true,
                gridcolor: '#E5E5E5'
              },
              margin: { t: 40, r: 20, l: 80, b: 80 }
            }}
            config={defaultConfig}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      case 'grouped_bar':
        const traces = plotData.property_types.map((pt, index) => ({
          type: 'bar',
          name: pt,
          x: plotData.districts,
          y: plotData.data[pt],
          marker: {
            color: plotData.colors[index]
          },
          hovertemplate: 
            "<b>%{x}</b><br>" +
            "Type: %{fullData.name}<br>" +
            "Price per Area: %{y:.2f} million VND/m²<extra></extra>"
        }));

        return (
          <Plot
            data={traces}
            layout={{
              ...layout,
              barmode: 'group',
              bargap: 0.15,
              bargroupgap: 0.1,
              margin: { t: 40, r: 20, l: 80, b: 130 },
              xaxis: { 
                ...layout.xaxis,
                tickangle: -45,
                automargin: true,
                title: {
                  text: "District",
                  font: { size: 14 },
                  standoff: 20
                }
              },
              yaxis: {
                ...layout.yaxis,
                tickformat: '.2f',
                title: {
                  text: "Price per Area (million VND/m²)",
                  font: { size: 14 },
                  standoff: 20
                }
              },
              legend: {
                orientation: 'h',
                yanchor: 'bottom',
                y: 0.96,
                xanchor: 'right',
                x: 1,
                title: {
                  text: "Property Type",
                  font: { size: 12 }
                }
              }
            }}
            config={defaultConfig}
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

        console.log('Scatter plotData:', plotData);
        console.log('Traces:', traces);
        return (
          <Plot
            data={traces}
            layout={{ ...layout }}
            config={defaultConfig}
            style={{ width: "100%", height: "100%" }}
            useResizeHandler={true}
          />
        );
      }

      case "scattermapbox": {
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
        console.log('Scattermapbox plotData:', plotData);
        console.log('Scattermapbox traces:', scatterTraces);

        return (
          <Plot
            data={scatterTraces}
            layout={{
              ...layout,
              mapbox: {
                style: "open-street-map",
                center: { lat: 21.0285, lon: 105.8542 },
                zoom: plotData.zoom || 11,
              },
              margin: { r: 0, t: 40, l: 0, b: 0 },
            }}
            config={defaultConfig}
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
                name: 'District Name',
              },
            ]}
            layout={{
              ...layout,
              mapbox: {
                style: 'open-street-map',
                center: plotData.center || { lat: 21.0285, lon: 105.8542 },
                zoom: plotData.zoom || 11,
              },
              margin: { r: 0, t: 40, l: 0, b: 0 },
              legend: {
                orientation: 'h',
                yanchor: 'bottom',
                y: 1.0,
                xanchor: 'left',
                x: 0.0,
              }
            }}
            config={defaultConfig}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
          />
        );

      default:
        console.warn('Unknown plot type:', plotData.type);
        return null;
    }
  }, []);

  // Memoize the grid items
  const gridItems = useMemo(() => {
    if (!data) return null;

    return (
      <>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.price_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.area_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.property_type_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.legal_status_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.district_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper elevation={3} sx={{ p: 2, height: 450, overflow: 'hidden' }}>
            {renderPlot(data.price_per_area_distribution)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 600, overflow: 'hidden' }}>
            {renderPlot(data.price_per_area_stats, 600)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 600, overflow: 'hidden' }}>
            {renderPlot(data.price_area_scatter, 600)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 600, overflow: 'hidden' }}>
            {mapLoaded && renderPlot(data.listings_map, 600)}
          </Paper>
        </Grid>
        <Grid item xs={12}>
          <Paper elevation={3} sx={{ p: 2, height: 700, overflow: 'hidden' }}>
            {mapLoaded && renderPlot(data.choropleth_map, 700)}
          </Paper>
        </Grid>
      </>
    );
  }, [data, mapLoaded, renderPlot]);

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

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Real Estate Data Visualizations
      </Typography>
      <Grid container spacing={3}>
        {gridItems}
      </Grid>
    </Box>
  );
};

export default React.memo(Visualizations);