import React from "react";

import AppLayout from "./components/AppLayout";
import DashboardPage from "./pages/DashboardPage";

const App: React.FC = () => {
  return (
    <AppLayout>
      <DashboardPage />
    </AppLayout>
  );
};

export default App;
