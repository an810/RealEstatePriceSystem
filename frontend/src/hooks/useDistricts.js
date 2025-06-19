import { useState, useEffect } from 'react';
import axios from 'axios';

export const useDistricts = () => {
  const [districts, setDistricts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDistricts = async () => {
      try {
        const response = await axios.get('http://localhost:8000/api/districts');
        // Sort districts by name
        const sortedDistricts = response.data.sort((a, b) => a.district.localeCompare(b.district));
        setDistricts(sortedDistricts.map(d => d.district));
      } catch (err) {
        setError(err.response?.data?.detail || 'Failed to fetch districts');
      } finally {
        setLoading(false);
      }
    };

    fetchDistricts();
  }, []);

  return { districts, loading, error };
}; 