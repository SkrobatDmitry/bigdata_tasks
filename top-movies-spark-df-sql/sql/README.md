# PySpark SQL
---
The script is used for searching movies in the CSV format file by the following parameters: genre, release year, title matches. Datasets are processed in distributed computing using SQL queries from PySpark DF/SQL.

## Usage
---
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]
```

### Parameters
---
- `-h, --help` - show help message and exit.
- `--N amount` - number of movies of each genre to output.
- `--genres genres` - requested movies genres.
- `--year-from year` - the first filter by the year the movie was made.
- `--year-to year` - the second filter by the year the movie was made.
- `--regxep regexp` - filter of movies title or their parts.

### Help message
---
Use `-h` or `--help` to get the help message. For example:
```bash
bash get-movies.sh --help
```

The script produces the following output:
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]

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
Use the `--N` argument to specify how many movies for each genre to show. For example:
```bash
bash setup_env.sh
bash get-movies.sh --N 1
```

In this case, the script displays one top movie for each genre:
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
Use the `--genres` argument to specify which genres movies should be displayed in. For example:
```bash
bash setup_envinronment.sh
bash get-movies.sh --N 2 --genres "Crime|IMAX|Thriller"
```

In this case, the script displays two top movies for each requested genre:
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
Specify `--year-from` or `--year-to` or both arguments to determine from which to which year the movies will be displayed. For example:
```bash
bash setup_envinronment.sh
bash get-movies.sh --N 3 --genres "Action|Animation" --year-from 1920 --year-to 1930
```

In this case, the script displays three top movies released between 1920 and 1930 for each requested genre:
```text
genre,title,year,rating
Action,Aelita: The Queen of Mars (Aelita),1924,4.5
Action,All Quiet on the Western Front,1930,4.35
Action,Safety Last!,1923,4.0
Animation,Steamboat Willie,1928,2.3333
```

### Regexp
---
Use the `--regexp` argument to determine if a title matches a given regular expression. For example:
```bash
bash setup_envinronment.sh
bash get-movies.sh --N 1 --regexp "Panda"
```

In this case, the script displays one top movie with Panda in the title for each genre:
```text
genre,title,year,rating
Action,Kung Fu Panda,2008,3.4444
Adventure,Kung Fu Panda 3,2016,3.375
Animation,Kung Fu Panda: Secrets of the Masters,2011,5.0
Children,Kung Fu Panda: Secrets of the Masters,2011,5.0
Comedy,Kung Fu Panda,2008,3.4444
IMAX,Kung Fu Panda,2008,3.4444
```