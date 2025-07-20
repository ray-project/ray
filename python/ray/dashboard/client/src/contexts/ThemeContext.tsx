import React, { createContext, ReactNode, useContext, useEffect, useState } from 'react';

type ThemeMode = 'light' | 'dark';

type ThemeContextType = {
  mode: ThemeMode;
  toggleTheme: () => void;
};

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

type ThemeProviderProps = {
  children: ReactNode;
};

export const ThemeProvider: React.FC<ThemeProviderProps> = ({ children }) => {
  const [mode, setMode] = useState<ThemeMode>(() => {
    // Get initial theme from localStorage or default to light
    try {
      const savedTheme = localStorage.getItem('ray-dashboard-theme') as ThemeMode;
      return savedTheme || 'light';
    } catch {
      // Handle cases where localStorage is not available (like in tests)
      return 'light';
    }
  });

  useEffect(() => {
    // Persist theme preference to localStorage
    try {
      localStorage.setItem('ray-dashboard-theme', mode);
    } catch {
      // Handle cases where localStorage is not available
    }
  }, [mode]);

  const toggleTheme = () => {
    setMode(prevMode => prevMode === 'light' ? 'dark' : 'light');
  };

  return (
    <ThemeContext.Provider value={{ mode, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  );
};

export const useTheme = (): ThemeContextType => {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
};