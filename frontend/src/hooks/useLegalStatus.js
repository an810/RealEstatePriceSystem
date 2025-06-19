import { useState, useEffect } from 'react';
import axios from 'axios';

export const useLegalStatus = () => {
  const [legalStatuses, setLegalStatuses] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchLegalStatuses = async () => {
      try {
        const response = await axios.get('http://localhost:8000/api/legal-status');
        // Sort legal statuses by name
        const sortedStatuses = response.data.sort((a, b) => a.legal.localeCompare(b.legal));
        setLegalStatuses(sortedStatuses.map(s => s.legal));
      } catch (err) {
        setError(err.response?.data?.detail || 'Failed to fetch legal statuses');
      } finally {
        setLoading(false);
      }
    };

    fetchLegalStatuses();
  }, []);

  return { legalStatuses, loading, error };
}; 