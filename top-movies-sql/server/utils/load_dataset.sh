#!/bin/bash
function load_dataset() {
  wget -q https://files.grouplens.org/datasets/movielens/$1.zip
  unzip -u -q $1.zip $1/movies.csv $1/ratings.csv

  if [ -d "dataset" ]; then
    rm -r dataset
  fi

  mv $1 dataset
  rm $1.zip
}