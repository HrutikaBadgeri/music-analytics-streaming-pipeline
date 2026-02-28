from analytics.connection import get_connection

def get_dau(selected_date):

    con = get_connection()

    query = f"""
        SELECT COUNT(DISTINCT user_id) as dau
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return con.execute(query).fetchone()[0]

def get_total_plays(selected_date):

    con = get_connection()

    query = f"""
        SELECT SUM(plays)
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return con.execute(query).fetchone()[0]

def get_heavy_user_pct(selected_date):

    con = get_connection()

    query = f"""
        SELECT 
            (SUM(heavy_engagement) * 100.0) / COUNT(*)
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    result = con.execute(query).fetchone()[0]

    return round(result, 2) if result else 0

# engagement charts 
# def get_engagement_trend():
#     con = get_connection()
#     query = """
#         SELECT
#             date,
#             SUM(plays) AS total_plays
#         FROM local.tracks_daily_summary
#         GROUP BY date
#         ORDER BY date
#     """
#     df = con.execute(query).df()
#     return df

def get_engagement_trend(days=None):
    conn = get_connection()

    query = """
        SELECT
            date,
            SUM(plays) AS total_plays
        FROM tracks_daily_summary
    """

    if days is not None:
        query += f"""
            WHERE date >= CURRENT_DATE - INTERVAL {days} DAY
        """

    query += """
        GROUP BY date
        ORDER BY date
    """

    return conn.execute(query).df()

def get_user_play_distribution(selected_date):
    conn = get_connection()

    query = f"""
        SELECT
            plays
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_user_session_distribution(selected_date):
    conn = get_connection()

    query = f"""SELECT sessions
            FROM user_daily_summary
            WHERE date = '{selected_date}'
            AND is_active = 1"""
    return conn.execute(query).df()

def get_top_tracks(selected_date, limit=10):
    conn = get_connection()

    query = f"""
    SELECT
        t.track_id,
        d.track_name,
        d.genre,
        d.language,
        d.artist_name,
        t.plays,
        t.unique_listeners,
    FROM tracks_daily_summary t
    JOIN dim_tracks d
        ON t.track_id = d.track_id
    WHERE t.date = '{selected_date}'
    ORDER BY t.plays DESC
    LIMIT {limit}"""

    top_tracks = conn.execute(query).df()
    return top_tracks

def get_most_loved_tracks(selected_date, limit=5):
    conn = get_connection()

    query = f"""
        SELECT
            t.track_id,
            d.track_name,
            t.likes,
            t.playlist_adds,
            (t.likes + t.playlist_adds) AS love_score
        FROM tracks_daily_summary t
        JOIN dim_tracks d
            ON TRIM(t.track_id) = TRIM(d.track_id)
        WHERE t.date = '{selected_date}'
        ORDER BY love_score DESC
        LIMIT {limit}
    """

    return conn.execute(query).df()
def get_track_engagement_reach(selected_date):
    conn = get_connection()

    query = f"""
        SELECT
            t.track_id,
            d.track_name,
            t.unique_listeners,
            t.engagement_score,
            t.plays,
            t.skip_rate
        FROM tracks_daily_summary t
        JOIN dim_tracks d
            ON TRIM(t.track_id) = TRIM(d.track_id)
        WHERE t.date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_user_behavior_kpis(selected_date):
    conn = get_connection()

    query = f"""
        SELECT
            AVG(explorer_flag) * 100 AS explorer_pct,
            AVG(multi_session_flag) * 100 AS multi_session_pct,
            AVG(heavy_engagement) * 100 AS heavy_engagement_pct,
            AVG(unique_tracks) AS avg_unique_tracks
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_user_play_distribution(selected_date):
    conn = get_connection()

    query = f"""
        SELECT plays
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_unique_track_distribution(selected_date):
    conn = get_connection()

    query = f"""
        SELECT unique_tracks
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_user_session_distribution(selected_date):
    conn = get_connection()

    query = f"""
        SELECT sessions
        FROM user_daily_summary
        WHERE date = '{selected_date}'
    """

    return conn.execute(query).df()

def get_plan_distribution():
    conn = get_connection()

    query = """
        SELECT
            plan_type,
            COUNT(*) AS user_count
        FROM dim_users
        GROUP BY plan_type
        ORDER BY user_count DESC
    """

    return conn.execute(query).df()