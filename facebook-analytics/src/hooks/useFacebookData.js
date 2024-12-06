// hooks/useFacebookData.js
import { useState, useEffect } from 'react';

const API_URL = 'https://jktob6rblf.execute-api.ap-southeast-1.amazonaws.com/';

export const useFacebookData = () => {
  const [posts, setPosts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchPosts = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_URL}/posts`);
      if (!response.ok) {
        throw new Error('Failed to fetch posts');
      }
      const data = await response.json();
      setPosts(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPosts();
  }, []);

  const refreshData = () => {
    fetchPosts();
  };

  return { posts, loading, error, refreshData };
};