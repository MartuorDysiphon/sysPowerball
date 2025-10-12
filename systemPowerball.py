def clean_data(self):
        """Clean and prepare the data for analysis with validation"""
        if self.data is None:
            print("No data to clean. Load data first.")
            return False

        self.data[self.date_column] = pd.to_datetime(self.data[self.date_column], errors='coerce')

        self.data = self.data.dropna(subset=[self.date_column])

        for col in self.white_ball_columns + [self.powerball_column]:
            self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

        for col in self.white_ball_columns:
            invalid_mask = ~self.data[col].between(self.white_ball_min, self.white_ball_max) & ~self.data[col].isna()
            if invalid_mask.any():
                invalid_count = invalid_mask.sum()
                print(
                    f"Warning: Found {invalid_count} invalid values in column {col} (outside range {self.white_ball_min}-{self.white_ball_max})")

        invalid_powerballs = ~self.data[self.powerball_column].between(self.powerball_min, self.powerball_max) & ~ \
        self.data[self.powerball_column].isna()
        if invalid_powerballs.any():
            invalid_count = invalid_powerballs.sum()
            print(
                f"Warning: Found {invalid_count} invalid Powerball values (outside range {self.powerball_min}-{self.powerball_max})")

        self.data = self.data.dropna(subset=self.white_ball_columns + [self.powerball_column])

        self.data = self.data.sort_values(by=self.date_column).reset_index(drop=True)

        print(f"Data cleaned. {len(self.data)} valid records remaining.")
        return True

    # def analyze_frequency(self):
        """Analyze frequency of numbers"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None, None

        white_balls = []
        for col in self.white_ball_columns:
            white_balls.extend(self.data[col].astype(int).tolist())

        # white_counter = Counter(white_balls)

        # powerball_counter = Counter(self.data[self.powerball_column].astype(int).tolist())

        # return white_counter, powerball_counter