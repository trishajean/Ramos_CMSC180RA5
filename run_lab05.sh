#!/bin/bash

echo "Starting Slave 1 on port 70001..."
./core_affine <<< $'1000\n70001\n1' &

echo "Starting Slave 2 on port 70002..."
./core_affine <<< $'1000\n70002\n1' &

# Give slaves time to set up and listen
sleep 1

echo "Starting Master..."
./core_affine <<< $'1000\n70000\n0\n2'
