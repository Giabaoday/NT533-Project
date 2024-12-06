import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const API_URL = 'https://jktob6rblf.execute-api.ap-southeast-1.amazonaws.com/';

const SimpleDashboard = () => {
  const [posts, setPosts] = useState([]);
  const [postHistory, setPostHistory] = useState([]); // Thêm state cho history
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedPost, setSelectedPost] = useState(null);

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

  const fetchPostHistory = async (postId) => {
    try {
      const response = await fetch(`${API_URL}/posts/${postId}/history`);
      if (!response.ok) {
        throw new Error('Failed to fetch post history');
      }
      const data = await response.json();
      setPostHistory(data);
    } catch (err) {
      console.error('Error fetching history:', err);
      setPostHistory([]);
    }
  };

  const handlePostSelect = (post) => {
    setSelectedPost(post);
    fetchPostHistory(post.post_id);
  };

  useEffect(() => {
    fetchPosts();
  }, []);

  if (loading) {
    return (
      <div className="flex bg-gray-100 min-h-screen items-center justify-center">
        <div className="bg-white p-6 rounded-lg shadow-lg">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-gray-600">Đang tải dữ liệu...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex bg-gray-100 min-h-screen items-center justify-center">
        <div className="bg-white p-6 rounded-lg shadow-lg">
          <p className="text-red-500 mb-4">Lỗi: {error}</p>
          <button
            onClick={fetchPosts}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Thử lại
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex bg-gray-100 min-h-screen">
      <div className="flex-1 p-6">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Facebook Analytics</h1>
          <button
            onClick={fetchPosts}
            className="px-4 py-2 border rounded-lg hover:bg-gray-50 flex items-center gap-2"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Làm mới
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          {/* Danh sách bài post */}
          <div className="md:col-span-1 space-y-4">
            <div className="bg-white rounded-lg shadow p-4">
              <h2 className="font-bold mb-4">Bài viết gần đây</h2>
              <div className="space-y-4">
                {posts.map(post => (
                  <div
                    key={post.post_id}
                    className={`p-4 border rounded-lg cursor-pointer transition-all ${selectedPost?.post_id === post.post_id
                      ? 'border-blue-500 bg-blue-50'
                      : 'hover:bg-gray-50'
                      }`}
                    onClick={() => handlePostSelect(post)}  // Sửa lại hàm click
                  >
                    <p className="text-sm font-medium mb-2">{post.content}</p>
                    <div className="flex justify-between text-sm text-gray-500">
                      <span>{new Date(post.created_time).toLocaleDateString()}</span>
                      <span className="flex items-center gap-1">
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                        </svg>
                        {post.total_comments}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Chi tiết và phân tích */}
          <div className="md:col-span-3">
            {selectedPost ? (
              <div className="space-y-6">
                {/* Thống kê giữ nguyên */}
                <div className="grid grid-cols-3 gap-4">
                  <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-sm font-medium text-gray-500 mb-1">Tổng comments</h3>
                    <p className="text-2xl font-bold">{selectedPost.total_comments}</p>
                  </div>
                  <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-sm font-medium text-gray-500 mb-1">Sentiment TB</h3>
                    <p className="text-2xl font-bold">{selectedPost.average_sentiment?.toFixed(1) || '0.0'}</p>
                  </div>
                  <div className="bg-white rounded-lg shadow p-4">
                    <h3 className="text-sm font-medium text-gray-500 mb-1">Tổng Sentiment</h3>
                    <p className="text-2xl font-bold">{selectedPost.sentiment_sum?.toFixed(1) || '0.0'}</p>
                  </div>
                </div>

                {/* Biểu đồ - Cập nhật để sử dụng postHistory */}
                <div className="bg-white rounded-lg shadow p-4">
                  <h3 className="font-bold mb-4">Xu hướng Sentiment</h3>
                  <div className="w-full">
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart
                        data={postHistory.map(item => ({
                          date: new Date(item.last_updated).toLocaleString(),
                          sentiment: item.average_sentiment
                        }))}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                          dataKey="date"
                          tick={{ fontSize: 12 }}
                        />
                        <YAxis domain={[0, 10]} />
                        <Tooltip />
                        <Legend />
                        <Line
                          type="monotone"
                          dataKey="sentiment"
                          stroke="#3b82f6"
                          name="Điểm Sentiment"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                {/* Nội dung bài viết giữ nguyên */}
                <div className="bg-white rounded-lg shadow p-4">
                  <h3 className="font-bold mb-4">Nội dung bài viết</h3>
                  <p>{selectedPost.content}</p>
                  <div className="mt-4 text-sm text-gray-500">
                    Đăng lúc: {new Date(selectedPost.created_time).toLocaleString()}
                  </div>
                </div>
              </div>
            ) : (
              <div className="h-full flex items-center justify-center">
                <p className="text-gray-500">Chọn một bài viết để xem chi tiết</p>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="w-32 bg-gray-100" />
    </div>
  );
};

export default SimpleDashboard;