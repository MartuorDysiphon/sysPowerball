import pandas as pd
import numpy as np
from collections import Counter
import os
from datetime import datetime
import warnings
import random

warnings.filterwarnings('ignore')


class SouthAfricanPowerballPredictor:
    def __init__(self):
        self.data = None
        self.white_ball_columns = [] 
        self.powerball_column = None 
        self.date_column = None  
        self.day_column = None  
        self.white_ball_min = 1
        self.white_ball_max = 50
        self.powerball_min = 1
        self.powerball_max = 20
        self.expected_files = [
            'powerballold.xlsx','powerball2018.xlsx', 'powerball2019.xlsx', 'powerball2020.xlsx',
            'powerball2021.xlsx', 'powerball2022.xlsx', 'powerball2023.xlsx',
            'powerball2024.xlsx', 'powerball2025.xlsx',
            'powerball_plus2018.xlsx', 'powerball_plus2019.xlsx', 'powerball_plus2020.xlsx',
            'powerball_plus2021.xlsx', 'powerball_plus2022.xlsx', 'powerball_plus2023.xlsx',
            'powerball_plus2024.xlsx', 'powerball_plus2025.xlsx'
        ]
        self.history_file = 'powerball_predictions_history.xlsx'

    def detect_columns(self, df):
        """Detect the column structure of the Excel file"""
        columns = df.columns.tolist()
        date_col = None
        day_col = None
        number_cols = []
        powerball_col = None

        # Xolumn identity
        for col in columns:
            col_lower = str(col).lower()

            if date_col is None and any(keyword in col_lower for keyword in ['date', 'draw date']):
                date_col = col
            elif day_col is None and any(keyword in col_lower for keyword in ['day', 'weekday']):
                day_col = col
            elif powerball_col is None and any(keyword in col_lower for keyword in ['powerball', 'pb', 'bonus']):
                powerball_col = col
            elif any(keyword in col_lower for keyword in ['no', 'num', 'ball', 'number']):
                number_cols.append(col)

        if date_col is None:
            for col in columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    date_col = col
                    break

        if len(number_cols) < 5:
            numeric_cols = []
            for col in columns:
                if pd.api.types.is_numeric_dtype(df[col]) and col != date_col and col != day_col:
                    numeric_cols.append(col)

            # Sort numeric columns
            numeric_cols.sort()
            if len(numeric_cols) >= 6:
                number_cols = numeric_cols[:5]
                powerball_col = numeric_cols[5]
            elif len(numeric_cols) >= 5:
                number_cols = numeric_cols[:5]
                if powerball_col is None and len(numeric_cols) >= 6:
                    powerball_col = numeric_cols[5]

        if date_col is None and len(columns) > 0:
            date_col = columns[0]

        if day_col is None and len(columns) > 1:
            day_col = columns[1]

        if len(number_cols) < 5 and len(columns) >= 7:
            number_cols = columns[2:7]

        if powerball_col is None and len(columns) >= 8:
            powerball_col = columns[7]

        return date_col, day_col, number_cols, powerball_col

    def load_data(self, folder_path):
        """Load all Excel files from the specified folder"""
        all_data = []
        missing_files = []

        if not os.path.exists(folder_path):
            print(f"Error: Folder '{folder_path}' does not exist.")
            return False

        for file_name in self.expected_files:
            file_path = os.path.join(folder_path, file_name)
            if not os.path.exists(file_path):
                missing_files.append(file_name)
                continue

            try:
                df = pd.read_excel(file_path)

                date_col, day_col, number_cols, powerball_col = self.detect_columns(df)

                if not date_col or len(number_cols) < 5 or not powerball_col:
                    print(f"Warning: Could not detect proper column structure in {file_name}. Skipping.")
                    continue

                df = df[[date_col, day_col] + number_cols + [powerball_col]].copy()

                df.columns = ['Date', 'Day'] + [f'No{i + 1}' for i in range(len(number_cols))] + ['Powerball']

                all_data.append(df)
                print(f"Loaded data from {file_name} ({len(df)} records)")

            except Exception as e:
                print(f"Error reading {file_name}: {str(e)}")

        if missing_files:
            print(f"\nWarning: The following expected files were not found:")
            for file in missing_files:
                print(f"  - {file}")

        if all_data:
            self.data = pd.concat(all_data, ignore_index=True)
            self.date_column = 'Date'
            self.day_column = 'Day'
            self.white_ball_columns = ['No1', 'No2', 'No3', 'No4', 'No5']
            self.powerball_column = 'Powerball'

            print(f"\nTotal records loaded: {len(self.data)}")
            return True
        else:
            print("No data was successfully loaded.")
            return False

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

    def analyze_frequency(self):
        """Analyze frequency of numbers"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None, None

        white_balls = []
        for col in self.white_ball_columns:
            white_balls.extend(self.data[col].astype(int).tolist())

        white_counter = Counter(white_balls)

        powerball_counter = Counter(self.data[self.powerball_column].astype(int).tolist())

        return white_counter, powerball_counter

    def calculate_differences(self):
        """Calculate differences between consecutive numbers in each draw"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        all_differences = []

        for _, row in self.data.iterrows():
            numbers = sorted([row[col] for col in self.white_ball_columns])
            differences = [numbers[i + 1] - numbers[i] for i in range(len(numbers) - 1)]
            all_differences.extend(differences)

        return Counter(all_differences)

    def find_repeating_patterns(self):
        """Find numbers that often appear together"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        pair_counter = Counter()

        for _, row in self.data.iterrows():
            numbers = sorted([int(row[col]) for col in self.white_ball_columns])
            for i in range(len(numbers)):
                for j in range(i + 1, len(numbers)):
                    pair = (numbers[i], numbers[j])
                    pair_counter[pair] += 1

        return pair_counter

    def analyze_trends(self):
        """Analyze trends over time"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        trends = {}

        for i, col in enumerate(self.white_ball_columns):
            trends[f'White_{i + 1}'] = self.data[col].rolling(window=10, min_periods=1).mean().iloc[-1]

        trends['Powerball'] = self.data[self.powerball_column].rolling(window=10, min_periods=1).mean().iloc[-1]

        return trends

    def analyze_duality(self):
        """Analyze numbers that often appear together (duality)"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        co_occurrence = np.zeros((self.white_ball_max + 1, self.white_ball_max + 1))

        for _, row in self.data.iterrows():
            numbers = [int(row[col]) for col in self.white_ball_columns]
            for i in range(len(numbers)):
                for j in range(i + 1, len(numbers)):
                    n1, n2 = numbers[i], numbers[j]
                    co_occurrence[n1][n2] += 1
                    co_occurrence[n2][n1] += 1

        return co_occurrence

    def analyze_skip_patterns(self):
        """Analyze how many draws a number skips before appearing again"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        last_occurrence = {}
        skip_patterns = {i: [] for i in range(1, self.white_ball_max + 1)}

        for idx, row in self.data.iterrows():
            numbers = [int(row[col]) for col in self.white_ball_columns]
            for num in numbers:
                if num in last_occurrence:
                    skip_count = idx - last_occurrence[num]
                    skip_patterns[num].append(skip_count)
                last_occurrence[num] = idx

        # average skips
        avg_skips = {}
        for num, skips in skip_patterns.items():
            if skips:
                avg_skips[num] = sum(skips) / len(skips)
            else:
                avg_skips[num] = 0

        return avg_skips

    def analyze_sum_statistics(self):
        """Analyze the sum of white balls in each draw"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        sums = []
        for _, row in self.data.iterrows():
            white_sum = sum([int(row[col]) for col in self.white_ball_columns])
            sums.append(white_sum)

        return {
            'min': min(sums),
            'max': max(sums),
            'mean': np.mean(sums),
            'median': np.median(sums),
            'std': np.std(sums),
            'mode': Counter(sums).most_common(1)[0][0] if sums else 0
        }

    def analyze_odd_even_ratio(self):
        """Analyze the ratio of odd to even numbers in draws"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        odd_even_stats = []
        for _, row in self.data.iterrows():
            numbers = [int(row[col]) for col in self.white_ball_columns]
            odd_count = sum(1 for n in numbers if n % 2 == 1)
            even_count = 5 - odd_count
            odd_even_stats.append((odd_count, even_count))

        avg_odd = sum(item[0] for item in odd_even_stats) / len(odd_even_stats)
        avg_even = sum(item[1] for item in odd_even_stats) / len(odd_even_stats)

        return {
            'avg_odd': avg_odd,
            'avg_even': avg_even,
            'common_pattern': Counter(odd_even_stats).most_common(1)[0][0] if odd_even_stats else (0, 0)
        }

    def generate_prediction_set(self, num_predictions=15):
        """Generate multiple predictions based on different methods"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return None

        all_predictions = []
        white_counter, powerball_counter = self.analyze_frequency()
        avg_skips = self.analyze_skip_patterns()
        co_occurrence = self.analyze_duality()
        sum_stats = self.analyze_sum_statistics()
        odd_even_stats = self.analyze_odd_even_ratio()
        diff_counter = self.calculate_differences()

        for i in range(num_predictions):
            freq_whites = [num for num, _ in white_counter.most_common(20)]
            freq_powerball = [num for num, _ in powerball_counter.most_common(10)]

            recent_whites = []
            for col in self.white_ball_columns:
                recent_whites.extend(self.data[col].astype(int).tail(10).tolist())

            recent_counter = Counter(recent_whites)
            recent_whites = [num for num, _ in recent_counter.most_common(15)]

            recent_powerball = self.data[self.powerball_column].astype(int).tail(10).mode()
            recent_powerball = recent_powerball[0] if not recent_powerball.empty else freq_powerball[0]

            common_diffs = [diff for diff, _ in diff_counter.most_common(8)]

            target_sum = int(sum_stats['mean'])

            target_odd_count = round(odd_even_stats['avg_odd'])

            candidate_whites = set()

            candidate_whites.update(freq_whites[:10])

            candidate_whites.update(recent_whites[:8])

            overdue_numbers = sorted(avg_skips.items(), key=lambda x: x[1], reverse=True)[:8]
            candidate_whites.update([num for num, _ in overdue_numbers])

            candidate_whites = [num for num in candidate_whites if self.white_ball_min <= num <= self.white_ball_max]

            if len(candidate_whites) >= 5:
                scores = {}
                for num in candidate_whites:
                    freq_score = white_counter[num] / len(self.data)

                    recent_score = 1 if num in recent_whites[:10] else 0.5 if num in recent_whites else 0

                    duality_score = 0
                    for other_num in candidate_whites:
                        if other_num != num:
                            duality_score += co_occurrence[num][other_num]
                    duality_score = duality_score / (len(candidate_whites) - 1) if len(candidate_whites) > 1 else 0

                    skip_score = avg_skips[num] / max(avg_skips.values()) if max(avg_skips.values()) > 0 else 0

                    scores[num] = freq_score * 0.3 + recent_score * 0.25 + duality_score * 0.25 + skip_score * 0.2

                top_candidates = sorted(scores.items(), key=lambda x: x[1], reverse=True)
                final_whites = [num for num, _ in top_candidates[:10]]  

                odd_count = sum(1 for n in final_whites if n % 2 == 1)
                even_count = len(final_whites) - odd_count

                if odd_count < target_odd_count:
                    odd_candidates = [n for n in candidate_whites if n % 2 == 1 and n not in final_whites]
                    odd_candidates.sort(key=lambda x: scores.get(x, 0), reverse=True)
                    final_whites.extend(odd_candidates[:min(target_odd_count - odd_count, len(odd_candidates))])
                elif odd_count > target_odd_count:
                    even_candidates = [n for n in candidate_whites if n % 2 == 0 and n not in final_whites]
                    even_candidates.sort(key=lambda x: scores.get(x, 0), reverse=True)
                    final_whites.extend(even_candidates[:min(odd_count - target_odd_count, len(even_candidates))])

                current_sum = sum(final_whites)
                if current_sum < target_sum - 10:
                    high_candidates = [n for n in candidate_whites if
                                       n > np.median(final_whites) and n not in final_whites]
                    high_candidates.sort(key=lambda x: scores.get(x, 0), reverse=True)
                    final_whites.extend(high_candidates[:min(2, len(high_candidates))])
                elif current_sum > target_sum + 10:
                    low_candidates = [n for n in candidate_whites if
                                      n < np.median(final_whites) and n not in final_whites]
                    low_candidates.sort(key=lambda x: scores.get(x, 0), reverse=True)
                    final_whites.extend(low_candidates[:min(2, len(low_candidates))])

                final_whites = sorted([num for num, _ in top_candidates[:5]])
            else:
                final_whites = sorted(candidate_whites)
                needed = 5 - len(final_whites)
                if needed > 0:
                    supplement = []
                    freq_list = [num for num, _ in white_counter.most_common(20) if num not in final_whites]
                    while len(supplement) < needed and freq_list:
                        num = freq_list.pop(0)
                        supplement.append(num)
                    final_whites.extend(supplement)
                    final_whites = sorted(final_whites)

            powerball_candidates = [
                freq_powerball[0],
                recent_powerball,
                self.data[self.powerball_column].astype(int).iloc[-1], 
                self.data[self.powerball_column].astype(int).iloc[-2] if len(self.data) > 1 else recent_powerball
            ]

            valid_powerballs = [pb for pb in powerball_candidates if self.powerball_min <= pb <= self.powerball_max]
            if not valid_powerballs:
                valid_powerballs = [num for num, _ in powerball_counter.most_common(5)]

            final_powerball = np.random.choice(valid_powerballs) if valid_powerballs else np.random.randint(
                self.powerball_min, self.powerball_max + 1)

            all_predictions.append({
                'white_balls': final_whites,
                'powerball': final_powerball,
                'frequent_whites': freq_whites[:5],
                'frequent_powerball': freq_powerball[0],
                'recent_whites': recent_whites[:5],
                'recent_powerball': recent_powerball,
                'overdue_numbers': [num for num, _ in overdue_numbers[:3]],
                'common_differences': common_diffs,
                'sum_statistics': sum_stats,
                'odd_even_stats': odd_even_stats
            })

        return all_predictions

    def save_prediction_to_history(self, prediction):
        """Save the prediction to history Excel file"""
        try:
            now = datetime.now()
            date_str = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H:%M:%S")
            day_str = now.strftime("%A")

            new_row = {
                'Date': date_str,
                'Time': time_str,
                'Day': day_str,
                'No1': prediction['white_balls'][0],
                'No2': prediction['white_balls'][1],
                'No3': prediction['white_balls'][2],
                'No4': prediction['white_balls'][3],
                'No5': prediction['white_balls'][4],
                'Powerball': prediction['powerball']
            }

            if os.path.exists(self.history_file):
                history_df = pd.read_excel(self.history_file)
            else:
                history_df = pd.DataFrame(
                    columns=['Date', 'Time', 'Day', 'No1', 'No2', 'No3', 'No4', 'No5', 'Powerball'])

            history_df = pd.concat([history_df, pd.DataFrame([new_row])], ignore_index=True)

            history_df.to_excel(self.history_file, index=False)
            print(f"Prediction saved to {self.history_file}")

        except Exception as e:
            print(f"Error saving prediction to history: {str(e)}")

    def print_analysis(self):
        """Print detailed analysis of the data"""
        if self.data is None:
            print("No data to analyze. Load data first.")
            return

        white_counter, powerball_counter = self.analyze_frequency()
        diff_counter = self.calculate_differences()
        pair_counter = self.find_repeating_patterns()
        trends = self.analyze_trends()
        avg_skips = self.analyze_skip_patterns()
        sum_stats = self.analyze_sum_statistics()
        odd_even_stats = self.analyze_odd_even_ratio()

        print("=" * 70)
        print("SOUTH AFRICAN POWERBALL ANALYSIS REPORT")
        print("=" * 70)
        print(f"Data range: {self.data[self.date_column].min().date()} to {self.data[self.date_column].max().date()}")
        print(f"Total draws analyzed: {len(self.data)}")
        print()

        print("Most common white balls (1-50):")
        for num, count in white_counter.most_common(15):
            print(f"  {num}: {count} times ({count / len(self.data) * 100:.1f}%)")

        print()
        print("Most common powerballs (1-20):")
        for num, count in powerball_counter.most_common(10):
            print(f"  {num}: {count} times ({count / len(self.data) * 100:.1f}%)")

        print()
        print("Most common differences between white balls:")
        for diff, count in diff_counter.most_common(8):
            print(f"  {diff}: {count} times")

        print()
        print("Most common number pairs:")
        for (num1, num2), count in pair_counter.most_common(8):
            print(f"  {num1}-{num2}: {count} times")

        print()
        print("Numbers with longest average skip (most overdue):")
        overdue = sorted(avg_skips.items(), key=lambda x: x[1], reverse=True)[:8]
        for num, avg_skip in overdue:
            print(f"  {num}: {avg_skip:.1f} draws on average between appearances")

        print()
        print("Sum statistics of white balls:")
        print(f"  Minimum sum: {sum_stats['min']}")
        print(f"  Maximum sum: {sum_stats['max']}")
        print(f"  Average sum: {sum_stats['mean']:.1f}")
        print(f"  Median sum: {sum_stats['median']}")
        print(f"  Most common sum: {sum_stats['mode']}")
        print(f"  Standard deviation: {sum_stats['std']:.1f}")

        print()
        print("Odd-Even ratio statistics:")
        print(f"  Average odd numbers per draw: {odd_even_stats['avg_odd']:.2f}")
        print(f"  Average even numbers per draw: {odd_even_stats['avg_even']:.2f}")
        print(
            f"  Most common pattern: {odd_even_stats['common_pattern'][0]} odd, {odd_even_stats['common_pattern'][1]} even")

        print()
        print("Recent trends (10-draw moving average):")
        trends_data = self.analyze_trends()
        if trends_data:
            for key, value in trends_data.items():
                print(f"  {key}: {value:.1f}")


def main():
    """Main function to run the South African Powerball predictor"""
    predictor = SouthAfricanPowerballPredictor()

    data_folder = 'powerball_data'

    if not predictor.load_data(data_folder):
        print("\nPlease make sure to:")
        print("1. Create a folder named 'powerball_data' in the same directory as this script")
        print("2. Place all Powerball and Powerball Plus Excel files in this folder")
        print("3. Files should be named:")
        for file_name in predictor.expected_files:
            print(f"   - {file_name}")
        return

    if not predictor.clean_data():
        return

    predictor.print_analysis()

    print("\n" + "=" * 70)
    print("GENERATING MULTIPLE PREDICTIONS")
    print("=" * 70)

    num_predictions = 15
    all_predictions = predictor.generate_prediction_set(num_predictions)

    if all_predictions:
        print(f"Generated {len(all_predictions)} predictions:")
        print("-" * 70)
        for i, prediction in enumerate(all_predictions, 1):
            print(
                f"Prediction {i:2d}: White balls: {sorted(prediction['white_balls'])} | Powerball: {prediction['powerball']}")

        selected_prediction = random.choice(all_predictions)

        print("\n" + "=" * 70)
        print("RANDOMLY SELECTED PREDICTION FOR NEXT DRAW")
        print("=" * 70)
        print(f"Selected white balls: {sorted(selected_prediction['white_balls'])}")
        print(f"Selected powerball: {selected_prediction['powerball']}")

        valid_whites = all(
            predictor.white_ball_min <= num <= predictor.white_ball_max for num in selected_prediction['white_balls'])
        valid_powerball = predictor.powerball_min <= selected_prediction['powerball'] <= predictor.powerball_max
        unique_whites = len(selected_prediction['white_balls']) == len(set(selected_prediction['white_balls']))

        print()
        print("Validation:")
        print(f"  White balls in valid range (1-50): {'Yes' if valid_whites else 'No'}")
        print(f"  Powerball in valid range (1-20): {'Yes' if valid_powerball else 'No'}")
        print(f"  All white balls are unique: {'Yes' if unique_whites else 'No'}")

        predicted_sum = sum(selected_prediction['white_balls'])
        print(f"  Sum of white balls: {predicted_sum} (Average: {selected_prediction['sum_statistics']['mean']:.1f})")

        odd_count = sum(1 for n in selected_prediction['white_balls'] if n % 2 == 1)
        even_count = 5 - odd_count
        print(
            f"  Odd-Even ratio: {odd_count} odd, {even_count} even (Target: {selected_prediction['odd_even_stats']['common_pattern'][0]} odd, {selected_prediction['odd_even_stats']['common_pattern'][1]} even)")

        predictor.save_prediction_to_history(selected_prediction)
    else:
        print("Failed to generate predictions.")


if __name__ == "__main__":
    main()