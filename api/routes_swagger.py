from api import app, request_handler, bcrypt



def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token')

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        except jwt.exceptions.InvalidTokenError as e:
            print("Exception:", e)
            return jsonify({'message': 'Token is invalid!'}), 401


        return f(*args, **kwargs)

    return decorated


@app.route('/api/login', methods=['POST'])
def login():
    """
    User Login
    Allows users to log in by providing a username and password and receive a JWT token.
    ---
    tags:
      - Authentication
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        description: The user's username and password
        required: true
        schema:
          type: object
          required:
            - username
            - password
          properties:
            username:
              type: string
              example: user123
            password:
              type: string
              example: password123
    responses:
      200:
        description: Login successful, returns JWT token.
        schema:
          type: object
          properties:
            token:
              type: string
              example: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjoidXNlcjEyMyIsImV4cCI6MTYzMzU2Nzg2NX0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c
      401:
        description: Invalid username or password.
    """
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    # Dummy user for demonstration purposes
    expected_username = 'user123'
    expected_password = 'password123'

    # Check if the provided credentials match the expected ones
    if username == expected_username and password == expected_password:
        # Generate JWT token
        token = jwt.encode({'user': username, 'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=300)}, app.config['SECRET_KEY'])
        return jsonify({'token': token})

    return jsonify({'message': 'Invalid username or password'}), 401



@app.route('/api/test', methods=['POST'])
@token_required
def process_data():
    """
    Process Data
    Receives and processes client data, then returns the results.
    ---
    tags:
      - Data Processing
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        description: Client data
        required: true
        schema:
          $ref: '#/definitions/ClientData'
    responses:
      200:
        description: Data processed successfully, results returned.
        schema:
          $ref: '#/definitions/ProcessResponse'
      401:
        description: Unauthorized access.
    """
    input_data = request.json

    # Extracting necessary information from input data
    client_id = input_data.get("Client ID")
    portfolio_name = input_data.get("Portfolio Name")
    portfolio_id = input_data.get("Portfolio ID")
    report_date = input_data.get("Report Date")
    risk_horizon = input_data.get("Risk Horizon")
    confidence_level = input_data.get("Confidence Level")
    benchmark = input_data.get("Benchmark")
    benchmark_expected_return = input_data.get("Benchmark Expected Return")
    horizon_days = input_data.get("Horizon Days")
    positions = input_data.get("Positions")

    # Dummy hardcoded response
    response = {
        "Client ID": client_id,
        "Portfolio Name": portfolio_name,
        "Portfolio ID": portfolio_id,
        "Report Date": report_date,
        "Risk Horizon": risk_horizon,
        "Confidence Level": confidence_level,
        "Benchmark": benchmark,
        "Benchmark Expected Return": benchmark_expected_return,
        "Horizon Days": horizon_days,
        "Positions": positions,
        "Summary VaR": {
            "Header": "col1,col2,col3",
            "Data": [
                "1,2,3",
                "1,2,3"
            ]
        },
        "Position VaR": {
            "Header": "col1,col2,col3",
            "Data": [
                "1,2,3",
                "1,2,3"
            ]
        },
        "Top Risk": {
            "Header": "col1,col2,col3",
            "Data": [
                "1,2,3",
                "1,2,3"
            ]
        }
    }

    return jsonify(response)


@app.route('/api/data_request', methods=['POST'])
@token_required
def data_request():
    """
    Data Request
    Returns respective market data or other types of data based on request type.
    ---
    tags:
      - Data Request
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        description: Details of the data request
        required: true
        schema:
          type: object
          properties:
            Request:
              type: string
              example: MarketData
    responses:
      200:
        description: Request successful, required data returned.
        schema:
          type: object
          properties:
            data:
              type: array
              items:
                type: string
      400:
        description: Invalid request type.
    """
    print('data_request')
    input_data = request.json

    # Request Type
    request_type = input_data.get("Request")


    if request_type == "MarketData":
        response, status = rh.Request_MarketData(input_data)
    else:
        response, status = rh.Request_Unknown(input_data)

    return jsonify(response)

