-- Create player_stats table in Supabase
CREATE TABLE player_stats (
    id BIGSERIAL PRIMARY KEY,
    player_name TEXT,
    player_display_name TEXT,
    position TEXT,
    position_group TEXT,
    recent_team TEXT,
    passing_yards REAL,
    passing_tds REAL,
    passing_interceptions REAL,
    sacks_suffered REAL,
    sack_fumbles_lost REAL,
    passing_2pt_conversions REAL,
    rushing_yards REAL,
    rushing_tds REAL,
    rushing_fumbles_lost REAL,
    rushing_2pt_conversions REAL,
    receptions REAL,
    receiving_yards REAL,
    receiving_tds REAL,
    receiving_fumbles_lost REAL,
    receiving_2pt_conversions REAL,
    special_teams_tds REAL,
    def_tackles_solo REAL,
    def_tackle_assists REAL,
    def_tackles_for_loss REAL,
    def_fumbles_forced REAL,
    def_sacks REAL,
    def_interceptions REAL,
    def_pass_defended REAL,
    def_tds REAL,
    def_fumbles REAL,
    def_safeties REAL,
    fumble_recovery_opp REAL,
    fumble_recovery_tds REAL,
    punt_return_yards REAL,
    kickoff_return_yards REAL,
    fg_made REAL,
    fg_att REAL,
    fg_made_distance TEXT,
    pat_made REAL,
    pat_att REAL,
    fantasy_points REAL,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Create an index on fantasy_points for faster queries
CREATE INDEX idx_fantasy_points ON player_stats(fantasy_points DESC);

-- Create an index on position_group for filtering
CREATE INDEX idx_position_group ON player_stats(position_group);
