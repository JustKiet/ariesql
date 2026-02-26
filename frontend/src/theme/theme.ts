"use client";

import { createTheme } from "@mui/material/styles";

export const theme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#14B8A6" },      // Teal
    secondary: { main: "#EC4899" },     // Pink
    success: { main: "#10B981" },       // Emerald
    warning: { main: "#F59E0B" },       // Amber
    info: { main: "#7C8CF8" },          // Lavender
    error: { main: "#EF4444" },         // Red
    background: {
      default: "#FAFBFE",
      paper: "#FFFFFF",
    },
    text: {
      primary: "#2D3142",
      secondary: "#7A7F96",
    },
  },
  typography: {
    fontFamily: "'Inter', 'Segoe UI', sans-serif",
  },
  shape: { borderRadius: 12 },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: "none",
        },
      },
    },
  },
});
