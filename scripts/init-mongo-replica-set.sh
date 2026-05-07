set -euo pipefail

wait_for() {
  local host="$1"
  local port="$2"

  until mongosh --host "$host" --port "$port" --quiet \
        --eval 'db.adminCommand({ ping: 1 }).ok' | grep 1 >/dev/null
  do
    sleep 1
  done
}

echo "Waiting for Mongo nodes..."
wait_for mongo1 27017
wait_for mongo2 27018
wait_for mongo3 27019

echo "Attempting replica set init..."
mongosh --host mongo1 --port 27017 --quiet <<'MONGO'
try {
  rs.status();
  print("Replica set already initialized");
} catch (e) {
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27018" },
      { _id: 2, host: "mongo3:27019" }
    ]
  });
  print("Replica set initialized");
}
MONGO
