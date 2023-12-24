#!/bin/bash

FRUIT=$1

if [ "$FRUIT" == "APPLE" ]; then
    echo "You selected Apple"

elif [ "$FRUIT" == "ORANGE" ]; then
    echo "You selected ORANGE"

elif [ "$FRUIT" == "GRAPE" ]; then
    echo "You selected GRAPE"

else
    echo "You selected other Fruit"

fi
