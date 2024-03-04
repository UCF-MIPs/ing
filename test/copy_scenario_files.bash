#!/bin/bash

for dir in ./OUTPUTS/scenarios/* 
do
    target=s3://mips-phase-2/scenarios/
    echo $dir
    scname=$(basename $dir)
    echo "$target$scname/processed_data/actors_and_messages/v1/"
    aws s3 cp --recursive $dir "$target$scname/processed_data/actors_and_messages/v1/"
    echo ""
done
