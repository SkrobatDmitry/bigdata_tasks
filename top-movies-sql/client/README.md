# Client
---
The utility makes a query to the database to get top rated movies that meet the specified criteria for each requested genre.

## Installation
---
Your system must have:

- [Python](https://www.python.org/downloads/).
- [Required frameworks](https://bitbucket.org/coherentprojects/coherent-training-dmitry-skrobat/src/master/top-movies-sql/client/requirements.txt).

To install the required frameworks use:
```bash
pip install -r requirements.txt
```

## Config
---
This utility supports [config](https://bitbucket.org/coherentprojects/coherent-training-dmitry-skrobat/src/master/top-movies-sql/client/config/) files. Config has the following structure:

```text
config
├── config.yaml
│   ├── csv
│   │   ├── head
│   │   └── delimiter
│   ├── memory
│   │   ├── batch_size
│   │   └── memory_limit
│   └── default
│       ├── n
│       ├── year_from
│       ├── year_to
│       ├── regexp
│       └── genres
└── database_config.yaml
    ├── database
    │   ├── user
    │   ├── password
    │   ├── host
    │   ├── port
    │   └── db
    └── stored_procedure
        └── get_movies
```

## Usage
---
```text
usage: get-movies.py [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]
```

### Parameters
---
- `-h, --help` - show help message and exit.
- `--N amount` - number of movies of each rating to output.
- `--genres genres` - requested movies genres.
- `--year-from year` - the first filter by the year the movie was made.
- `--year-to year` - the second filter by the year the movie was made.
- `--regxep regexp` - filter of movies title or their parts.

### Help message
---
Use `-h` or `--help` to get the help message. For example:
```bash
python3 get-movies.py --help
```

The utility produces the following output:
```text
usage: get-movies.py [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]

This utility allows user to get information about top rated films

optional arguments:
  -h, --help        show this help message and exit

  --N amount        number of movies of each rating to output
  --genres genres   requested movies genres
  --year-from year  the first filter by the year the movie was made
  --year-to year    the second filter by the year the movie was made
  --regexp regexp   filter of movies title or their parts
```

### Number
---
Use `--N` argument to determine how many top rated movies for each requested genre to display. For example:
```bash
python3 get-movies.py --N 1
```

In this case, the utility displays one top rated movie for each genre:
```text
genre,title,year,rating
Action,Tokyo Tribe,2014,5.0
Adventure,Ice Age: The Great Egg-Scapade,2016,5.0
Animation,Loving Vincent,2017,5.0
Children,Ice Age: The Great Egg-Scapade,2016,5.0
Comedy,All Yours,2016,5.0
Crime,Loving Vincent,2017,5.0
Documentary,Won't You Be My Neighbor?,2018,5.0
Drama,Loving Vincent,2017,5.0
Fantasy,L.A. Slasher,2015,5.0
Film-Noir,Rififi (Du rififi chez les hommes),1955,4.75
Horror,The Girl with All the Gifts,2016,5.0
IMAX,Happy Feet Two,2011,5.0
Musical,Holy Motors,2012,5.0
Mystery,The Editor,2015,5.0
Romance,All Yours,2016,5.0
Sci-Fi,SORI: Voice from the Heart,2016,5.0
Thriller,The Girl with All the Gifts,2016,5.0
War,Battle For Sevastopol,2015,5.0
Western,Trinity and Sartana Are Coming,1972,5.0
```

### Genres
---
Use `--genres` argument to determine for which genres the top rated movies need to be displayed. For example:
```bash
python3 get-movies.py --N 2 --genres "Crime|IMAX|Thriller"
```

In this case, the utility displays the two top rated movies only for the each requested genres:
```text
genre,title,year,rating
Crime,Loving Vincent,2017,5.0
Crime,L.A. Slasher,2015,5.0
IMAX,Happy Feet Two,2011,5.0
IMAX,More,1998,5.0
Thriller,The Girl with All the Gifts,2016,5.0
Thriller,Hellbenders,2012,5.0
```

### Year
---
Specify `--year-from` or `--year-to` or both arguments to determine from which to which year the top rated movies will be displayed. For example:
```bash
python3 get-movies.py --N 4 --genres "Action|Film-Noir" --year-from 2000 --year-to 2005
```

In this case, the utility displays the four top rated movies released between 2000 and 2005 for the each requested genres:
```text
genre,title,year,rating
Action,Battle Royale 2: Requiem (Batoru rowaiaru II: Chinkonka),2003,5.0
Action,Sisters (Syostry),2001,5.0
Action,Dog Soldiers,2002,4.6667
Action,Tae Guk Gi: The Brotherhood of War (Taegukgi hwinalrimyeo),2004,4.5
Film-Noir,13 Tzameti,2005,4.5
Film-Noir,Brick,2005,3.875
Film-Noir,Sin City,2005,3.8571
Film-Noir,Mulholland Drive,2001,3.8431
```

### Regexp
---
Use `--regexp` argument to determine if the name matches the given regular expression. For example:
```bash
python3 get-movies.py --N 3 --genres "Action" --year-from 2000 --year-to 2012 --regexp "Panda"
```

In this case, the utility displays the three top rated movies released between 2000 and 2012 and having Panda in the title for the requested genre:
```text
genre,title,year,rating
Action,Kung Fu Panda,2008,3.4444
Action,Kung Fu Panda 2,2011,3.3235
Action,Kung Fu Panda: Secrets of the Furious Five,2008,2.875
```