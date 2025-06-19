import { useState, useEffect } from 'react';
import axios from 'axios';

export const usePropertyType = () => {
  const [propertyTypes, setPropertyTypes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchPropertyTypes = async () => {
      try {
        const response = await axios.get('http://localhost:8000/api/property-type');
        // Sort property types by name
        const sortedTypes = response.data.sort((a, b) => a.property_type.localeCompare(b.property_type));
        setPropertyTypes(sortedTypes.map(t => t.property_type));
      } catch (err) {
        setError(err.response?.data?.detail || 'Failed to fetch property types');
      } finally {
        setLoading(false);
      }
    };

    fetchPropertyTypes();
  }, []);

  return { propertyTypes, loading, error };
}; 