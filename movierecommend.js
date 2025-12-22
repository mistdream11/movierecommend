/**
 * express接收html传递的参数
 */
 
var  express=require('express');
var  bodyParser = require('body-parser')
const { spawn, spawnSync } = require('child_process');
var  app=express();
var mysql=require('mysql');
var http = require("http");
const path = require('path');
const genreLoader = require('./genreLoader');
app.set('view engine', 'html'); 
app.set('views', './views');
app.use(bodyParser.urlencoded({extended: false}))
app.use(bodyParser.json())

const MOVIES_CSV_PATH = process.env.MOVIES_CSV_PATH || '/home/hadoop/movierecommend/movies.csv';
genreLoader.loadGenres(MOVIES_CSV_PATH);

function pickRandomMovies(source, count) {
    if (!Array.isArray(source) || source.length === 0) {
        return [];
    }
    const pool = source.slice();
    const limit = Math.min(count, pool.length);
    const selection = [];
    for (let i = 0; i < limit; i++) {
        const index = Math.floor(Math.random() * pool.length);
        selection.push(pool.splice(index, 1)[0]);
    }
    return selection;
}

function renderPersonalRatingsPage(res, user, movies, selectedGenres) {
    const movieNumbers = 10;
    const movielist = pickRandomMovies(movies, movieNumbers);
    if (movielist.length === 0) {
        res.send('没有符合条件的电影，请重新选择类型');
        return;
    }
    app.set('view engine', 'jade');
    res.render('personalratings', {
        title: 'Welcome User',
        userid: user.userid,
        username: user.username,
        movieforpage: movielist,
        selectedGenres: selectedGenres || []
    });
    app.set('view engine', 'html');
}

function renderGenreSelectionPage(res, user, message) {
    const genres = genreLoader.getGenres();
    app.set('view engine', 'jade');
    res.render('genreselect', {
        title: '选择感兴趣的电影类型',
        userid: user.userid,
        username: user.username,
        genres: genres,
        message: message || null
    });
    app.set('view engine', 'html');
}

function renderModeChoicePage(res, user, selectedGenres, message) {
    const safeGenres = selectedGenres || [];
    const genresJson = Buffer.from(JSON.stringify(safeGenres), 'utf8').toString('base64');
    app.set('view engine', 'jade');
    res.render('modechoice', {
        title: '选择推荐模式',
        userid: user.userid,
        username: user.username,
        selectedGenres: safeGenres,
        selectedGenresPayload: genresJson,
        message: message || null
    });
    app.set('view engine', 'html');
}

function queryAsync(sql, params) {
    return new Promise((resolve, reject) => {
        connection.query(sql, params, function(err, rows) {
            if (err) {
                return reject(err);
            }
            resolve(rows || []);
        });
    });
}

function buildGenreFilter(genres) {
    if (!genres || genres.length === 0) {
        return { clause: '', params: [] };
    }
    const cleaned = genres
        .map((genre) => String(genre).trim())
        .filter((genre) => genre.length > 0);
    if (!cleaned.length) {
        return { clause: '', params: [] };
    }
    const clause = cleaned.map(() => 'genre LIKE ?').join(' OR ');
    const params = cleaned.map((genre) => `%${genre}%`);
    return { clause, params };
}

async function fetchMoviesByGenres(genres, limit) {
    const baseSQL = 'SELECT movieid,moviename,picture,movieurl,genre FROM movieinfo';
    const { clause, params } = buildGenreFilter(genres);
    if (!clause) {
        return [];
    }
    const sql = `${baseSQL} WHERE (${clause}) ORDER BY RAND() LIMIT ?`;
    return queryAsync(sql, [...params, limit]);
}

async function fetchMoviesWithoutGenres(genres, limit) {
    const baseSQL = 'SELECT movieid,moviename,picture,movieurl,genre FROM movieinfo';
    const { clause, params } = buildGenreFilter(genres);
    if (!clause) {
        // 如果没有选择标签，则无法定义非匹配集合
        return [];
    }
    const sql = `${baseSQL} WHERE (genre IS NULL OR genre = '' OR NOT (${clause})) ORDER BY RAND() LIMIT ?`;
    return queryAsync(sql, [...params, limit]);
}

async function fetchFallbackMovies(limit) {
    const baseSQL = 'SELECT movieid,moviename,picture,movieurl,genre FROM movieinfo ORDER BY RAND() LIMIT ?';
    return queryAsync(baseSQL, [limit]);
}

function padMoviesIfNeeded(movies, neededCount) {
    if (movies.length === 0) {
        return movies;
    }
    let index = 0;
    while (movies.length < neededCount) {
        movies.push(movies[index % movies.length]);
        index++;
    }
    return movies;
}
 
/**
 * 配置MySQL
 */
var connection = mysql.createConnection({
    host     : '127.0.0.1',
    user     : 'root',
    password : '123456',
    database : 'movierecommend',
    port:'3306'
});
connection.connect();

/**
 * 跳转到网站首页
 */
app.get('/',function (req,res) {
    res.sendFile('/home/hadoop/movierecommend/views/index.html');
})

/**
 * 跳转到电影列表页面
 */
app.get('/about.html',function (req,res) {
  res.sendFile('/home/hadoop/movierecommend/views/about.html');
})

/**
 * 跳转到登录页面
 */

app.get('/loginpage',function (req,res) {
  res.sendFile('/home/hadoop/movierecommend/views/loginpage.html',{title:'登录'});
})

 
/**
 * 实现登录验证功能
 */
app.post('/login',function (req,res) {
    var  name=req.body.username.trim();
    var pwd=req.body.pwd.trim();
    console.log('username:'+name+' password:'+pwd);
    
    var selectSQL = "select * from user where username = '"+name+"' and password = '"+pwd+"'";
    connection.query(selectSQL,function (err,rows,fields) {
        if (err) throw  err;
        
        if (rows.length === 0) {
            res.send("用户名或密码错误！");
            return;
        }

        const user = rows[0];
        const genres = genreLoader.getGenres();
        if (!genres || genres.length === 0) {
            res.status(500).send('无法读取电影类型标签，请检查 MOVIES_CSV_PATH 配置。');
            return;
        }
        
        renderGenreSelectionPage(res, user, null);
    });
});

/**
 * 跳转到注册页面
 */
 
app.get('/registerpage',function (req,res) {
  res.sendFile('/home/hadoop/movierecommend/views/registerpage.html',{title:'注册'});
})

/**
 * 跳转到类型选择页面（从推荐结果页面返回）
 */
app.get('/genreselect', function (req, res) {
  const userid = req.query.userid;
  const username = req.query.username;

  if (!userid || !username) {
    return res.status(400).send('缺少用户信息，请重新登录');
  }

  const user = { userid, username };
  renderGenreSelectionPage(res, user, null);
});

/**
 * 跳转到个人评分页面（从推荐结果页面继续评分）
 */
app.get('/personalratings', function (req, res) {
  const userid = req.query.userid;
  const username = req.query.username;

  if (!userid || !username) {
    return res.status(400).send('缺少用户信息，请重新登录');
  }

  const user = { userid, username };
  
  // 获取随机电影进行评分
  fetchFallbackMovies(10).then(movielist => {
    if (movielist.length === 0) {
      res.send('没有符合条件的电影，请重新选择类型');
      return;
    }

    app.set('view engine', 'jade');
    res.render('personalratings', {
      title: 'Welcome User',
      userid: user.userid,
      username: user.username,
      movieforpage: movielist,
      selectedGenres: []
    });
    app.set('view engine', 'html');
  }).catch(err => {
    console.error('Error fetching movies:', err);
    res.status(500).send('获取电影列表失败，请重试');
  });
});
 
app.post('/selectgenres', function (req, res) {
    const userid = req.body.userid;
    const username = req.body.username;
    let selectedGenres = req.body.genres;

    if (!userid || !username) {
        return res.status(400).send('缺少用户信息，请重新登录');
    }

    const user = { userid, username };

    const availableGenres = genreLoader.getGenres();
    if (!availableGenres || availableGenres.length === 0) {
        return res.status(500).send('无法读取电影类型标签，请检查 MOVIES_CSV_PATH 配置。');
    }

    if (!selectedGenres) {
        return renderGenreSelectionPage(res, user, '请至少选择一个类型标签');
    }

    if (!Array.isArray(selectedGenres)) {
        selectedGenres = [selectedGenres];
    }

    selectedGenres = selectedGenres
        .map((genre) => String(genre).trim())
        .filter((genre) => genre.length > 0);

    if (selectedGenres.length === 0) {
        return renderGenreSelectionPage(res, user, '请至少选择一个类型标签');
    }

    renderModeChoicePage(res, user, selectedGenres, null);
});

app.post('/chooserecommendation', async function(req, res) {
    const userid = req.body.userid;
    const username = req.body.username;
    const recommendMode = req.body.recommendMode;
    let selectedGenres = req.body.selectedGenres;

    if (!userid || !username) {
        return res.status(400).send('缺少用户信息，请重新登录');
    }

    try {
        if (typeof selectedGenres === 'string') {
            try {
                const decoded = Buffer.from(selectedGenres, 'base64').toString('utf8');
                selectedGenres = JSON.parse(decoded);
            } catch (err) {
                console.warn('解析标签载荷失败，尝试拆分字符串', err);
                selectedGenres = selectedGenres.split(',').map((item) => item.trim()).filter(Boolean);
            }
        }

        if (!Array.isArray(selectedGenres) || selectedGenres.length === 0) {
            return renderGenreSelectionPage(res, { userid, username }, '请重新选择类型');
        }

        const stepsNeeded = recommendMode === 'detailed' ? 10 : 5;
        const perCategoryPerStep = 5;
        const moviesNeeded = stepsNeeded * perCategoryPerStep;

        const matchingMovies = await fetchMoviesByGenres(selectedGenres, moviesNeeded);
        if (matchingMovies.length === 0) {
            return renderModeChoicePage(res, { userid, username }, selectedGenres, '没有找到包含所选标签的电影，请重新选择类型');
        }
        if (matchingMovies.length < moviesNeeded) {
            padMoviesIfNeeded(matchingMovies, moviesNeeded);
        }

        const fillerMovies = await fetchMoviesWithoutGenres(selectedGenres, moviesNeeded);
        if (fillerMovies.length === 0) {
            return renderModeChoicePage(res, { userid, username }, selectedGenres, '没有足够的其他类型电影，请减少标签或重新选择');
        }
        if (fillerMovies.length < moviesNeeded) {
            padMoviesIfNeeded(fillerMovies, moviesNeeded);
        }

        app.set('view engine', 'jade');
        res.render('interactiveRatings', {
            title: '电影偏好采集',
            userid,
            username,
            selectedGenres,
            modeLabel: recommendMode === 'detailed' ? '详细推荐' : '简单推荐',
            stepsNeeded,
            matchingMovies,
            fillerMovies
        });
        app.set('view engine', 'html');
    } catch (error) {
        console.error('生成推荐模式数据失败:', error);
        renderModeChoicePage(res, { userid, username }, selectedGenres, '生成推荐数据失败，请重试');
    }
});

/**
 * 实现注册功能
 */
app.post('/register',function (req,res) {
    var  name=req.body.username.trim();
    var  pwd=req.body.pwd.trim();
    var  user={username:name,password:pwd};
    connection.query('insert into user set ?',user,function (err,rs) {
        if (err) throw  err;
        console.log('register success');
       res.sendFile('/home/hadoop/movierecommend/views/registersuccess.html',{title:'注册成功',message:name});
    })
})

/**
 * 把用户评分写入数据库
 */
 
app.post('/submituserscore',function (req,res) {
    var userid=req.body.userid;
    var moviescores=[];
    var movieids=[];
    var incomingScores = req.body.moviescore;
    var incomingIds = req.body.movieid;

    if (!incomingScores || !incomingIds) {
        return res.status(400).send('未收到任何评分，请至少选择一个电影');
    }

    if (!Array.isArray(incomingScores)) {
        incomingScores = [incomingScores];
    }
    if (!Array.isArray(incomingIds)) {
        incomingIds = [incomingIds];
    }

    if (incomingScores.length !== incomingIds.length) {
        return res.status(400).send('评分数据不完整，请重试');
    }

    incomingScores.forEach(function(score){
        moviescores.push({moviescore:score});
    });
    incomingIds.forEach(function(id){
        movieids.push({movieid:id});
    });

    //删除该用户历史评分数据，为写入本次最新评分数据做准备
    connection.query('delete from  personalratings where userid='+userid, function(err, result) {
        if (err) throw err;
        console.log('deleted old personal ratings for user ' + userid);
    });
    
    //生成评分时间戳
    var mytimestamp =new Date().getTime().toString().slice(1,10);        
    console.log('timestamp: ' + mytimestamp);
    
    // 计数器用于检查所有插入是否完成
    var insertCount = 0;
    var totalInserts = movieids.length;
    
    if (totalInserts === 0) {
        // 如果没有评分数据，直接跳转
        var selectUserIdNameSQL='select userid,username from user where userid='+userid;
        connection.query(selectUserIdNameSQL,function(err,rows,fields){
           if (err) throw  err;
           app.set('view engine', 'jade'); 
           res.render('userscoresuccess',{title:'Personal Rating Success',user:rows[0]});
           app.set('view engine', 'html'); 
        });
        return;
    }
    
    for(var item in movieids){
       //把每条评分记录(userid,movieid,rating,timestamp)插入数据库  
       var personalratings={userid:userid,movieid:movieids[item].movieid,rating:moviescores[item].moviescore,timestamp:mytimestamp};
       connection.query('insert into personalratings set ?',personalratings,function (err,rs) {
        if (err) throw  err;
        insertCount++;
        console.log('insert into personalrating success for movie ' + personalratings.movieid);
        
        // 检查是否所有插入都完成
        if (insertCount === totalInserts) {
            var selectUserIdNameSQL='select userid,username from user where userid='+userid;
            connection.query(selectUserIdNameSQL,function(err,rows,fields){
               if (err) throw  err;
               app.set('view engine', 'jade'); 
               res.render('userscoresuccess',{title:'Personal Rating Success',user:rows[0]});
               app.set('view engine', 'html'); 
            });
        }
       });
    }
}); 

/**
 * 调用Python Spark程序为用户推荐电影并把推荐结果写入数据库,把推荐结果显示到网页
 */     
app.get('/recommendmovieforuser',function (req,res) {
    const userid=req.query.userid;
    const username=req.query.username;
    
    console.log('Starting recommendation for userid: ' + userid + ', username: ' + username);
    
    // Python脚本路径和参数
    const pythonScriptPath = '/home/hadoop/movierecommend/movie_rec/movie_rec.py';
    const dataDir = '/home/hadoop/ml-latest-small';
    
    console.log('Running Python script: ' + pythonScriptPath);
    console.log('Data directory: ' + dataDir);
    console.log('User ID: ' + userid);
    
    // 方法1: 使用spawnSync（同步，会阻塞，但更简单）
    try {
        console.log('Starting Python recommendation system...');
        
        // 设置环境变量以确保使用正确的Python
        const env = Object.assign({}, process.env, {
            PYSPARK_PYTHON: 'python3',
            PYSPARK_DRIVER_PYTHON: 'python3'
        });
        
        // 使用spawnSync同步执行Python脚本
        const result = spawnSync('python3', [pythonScriptPath, dataDir, userid], {
            encoding: 'utf-8',
            env: env,
            timeout: 300000, // 5分钟超时
            stdio: 'pipe'
        });
        
        console.log('Python script stdout:');
        console.log(result.stdout);
        
        if (result.stderr) {
            console.error('Python script stderr:');
            console.error(result.stderr);
        }
        
        if (result.error) {
            console.error('Spawn error:', result.error);
            return res.status(500).send('推荐系统启动失败: ' + result.error.message);
        }
        
        if (result.status !== 0) {
            console.error('Python script exited with code:', result.status);
            console.error('Error output:', result.stderr);
            return res.status(500).send('推荐系统运行失败，错误代码: ' + result.status + '，错误信息: ' + result.stderr);
        }
        
        console.log('Python script completed successfully with exit code:', result.status);
        
        // 从数据库中读取推荐结果
        var selectRecommendResultSQL = `
            SELECT recommendresult.userid, recommendresult.movieid, 
                   recommendresult.rating, movieinfo.moviename, movieinfo.picture, movieinfo.movieurl 
            FROM recommendresult 
            INNER JOIN movieinfo ON recommendresult.movieid = movieinfo.movieid 
            WHERE recommendresult.userid = ${userid}
            ORDER BY recommendresult.rating DESC
            LIMIT 10`;
        
        connection.query(selectRecommendResultSQL, function(err, rows, fields) {
            if (err) {
                console.error('Database query error: ' + err);
                return res.status(500).send('数据库查询失败');
            }
            
            console.log('Read ' + rows.length + ' recommendations from database');
            
            var movieinfolist = [];
            for (var i = 0; i < rows.length; i++) {
                movieinfolist.push({
                    userid: rows[i].userid,
                    movieid: rows[i].movieid,
                    rating: rows[i].rating ? rows[i].rating.toFixed(4) : '0.0000', // 格式化评分
                    moviename: rows[i].moviename,
                    picture: rows[i].picture,
                    movieurl: rows[i].movieurl || '#'
                });
                console.log('Recommendation ' + (i+1) + ': ' + rows[i].moviename + ' (rating: ' + rows[i].rating + ')');
            }
            
            if (movieinfolist.length === 0) {
                console.log('No recommendations found for user ' + userid);
                movieinfolist.push({
                    userid: userid,
                    movieid: 0,
                    rating: '0.0000',
                    moviename: '暂无推荐结果，请尝试评分更多电影',
                    picture: 'default.jpg'
                });
            }
            
            app.set('view engine', 'jade');
            res.render('recommendresult', {
                title: 'Recommend Result', 
                message: '为您推荐的电影', 
                username: username,
                userid: userid,
                movieinfo: movieinfolist
            });
            app.set('view engine', 'html');
        });
        
    } catch (error) {
        console.error('Error running recommendation system:', error);
        return res.status(500).send('推荐系统运行异常: ' + error.message);
    }
});

/**
 * 查看历史推荐结果（不重新计算）
 */
app.get('/viewrecommendations', function (req, res) {
    const userid = req.query.userid;
    const username = req.query.username;
    
    console.log('Viewing recommendations for user: ' + userid);
    
    var selectRecommendResultSQL = `
        SELECT recommendresult.userid, recommendresult.movieid, 
               recommendresult.rating, movieinfo.moviename, movieinfo.picture, movieinfo.movieurl 
        FROM recommendresult 
        INNER JOIN movieinfo ON recommendresult.movieid = movieinfo.movieid 
        WHERE recommendresult.userid = ${userid}
        ORDER BY recommendresult.rating DESC
        LIMIT 10`;
    
    connection.query(selectRecommendResultSQL, function(err, rows, fields) {
        if (err) {
            console.error('Database query error: ' + err);
            return res.status(500).send('数据库查询失败');
        }
        
        console.log('Found ' + rows.length + ' existing recommendations');
        
        var movieinfolist = [];
        for (var i = 0; i < rows.length; i++) {
            movieinfolist.push({
                userid: rows[i].userid,
                movieid: rows[i].movieid,
                rating: rows[i].rating ? rows[i].rating.toFixed(4) : '0.0000',
                moviename: rows[i].moviename,
                picture: rows[i].picture,
                movieurl: rows[i].movieurl
            });
        }
        
        if (movieinfolist.length === 0) {
            movieinfolist.push({
                userid: userid,
                movieid: 0,
                rating: '0.0000',
                moviename: '暂无历史推荐，请先进行电影推荐',
                picture: 'default.jpg'
            });
        }
        
        app.set('view engine', 'jade');
        res.render('recommendresult', {
            title: 'Recommend Result', 
            message: '您的历史推荐', 
            username: username,
            userid: userid,
            movieinfo: movieinfolist
        });
        app.set('view engine', 'html');
    });
});

/**
 * 清空用户推荐结果（用于测试）
 */
app.get('/clearrecommendations', function (req, res) {
    const userid = req.query.userid;
    
    if (!userid) {
        return res.status(400).send('用户ID不能为空');
    }
    
    connection.query('DELETE FROM recommendresult WHERE userid = ?', [userid], function(err, result) {
        if (err) {
            console.error('Error clearing recommendations: ' + err);
            return res.status(500).send('清除推荐结果失败');
        }
        
        console.log('Cleared recommendations for user ' + userid + ', affected rows: ' + result.affectedRows);
        res.send('已清除用户 ' + userid + ' 的推荐结果，影响行数: ' + result.affectedRows);
    });
});

/**
 * 查看用户评分记录
 */
app.get('/viewuserratings', function (req, res) {
    const userid = req.query.userid;
    const username = req.query.username;
    
    var selectRatingsSQL = `
        SELECT personalratings.userid, personalratings.movieid, 
               personalratings.rating, movieinfo.moviename, movieinfo.picture 
        FROM personalratings 
        INNER JOIN movieinfo ON personalratings.movieid = movieinfo.movieid 
        WHERE personalratings.userid = ${userid}
        ORDER BY personalratings.rating DESC`;
    
    connection.query(selectRatingsSQL, function(err, rows, fields) {
        if (err) {
            console.error('Database query error: ' + err);
            return res.status(500).send('数据库查询失败');
        }
        
        console.log('Found ' + rows.length + ' ratings for user ' + userid);
        
        var ratinglist = [];
        for (var i = 0; i < rows.length; i++) {
            ratinglist.push({
                userid: rows[i].userid,
                movieid: rows[i].movieid,
                rating: rows[i].rating,
                moviename: rows[i].moviename,
                picture: rows[i].picture
            });
        }
        
        app.set('view engine', 'jade');
        res.render('userratings', {
            title: 'User Ratings', 
            username: username, 
            ratings: ratinglist
        });
        app.set('view engine', 'html');
    });
});

/**
 * 系统健康检查
 */
app.get('/health', function (req, res) {
    // 检查数据库连接
    connection.query('SELECT 1', function(err, results) {
        if (err) {
            console.error('Database health check failed:', err);
            return res.status(500).json({
                status: 'error',
                message: 'Database connection failed',
                error: err.message
            });
        }
        
        // 检查Python脚本是否存在
        const fs = require('fs');
        const pythonScriptPath = '/home/hadoop/movierecommend/movie_rec/movie_rec.py';
        
        if (!fs.existsSync(pythonScriptPath)) {
            return res.status(500).json({
                status: 'error',
                message: 'Python script not found',
                path: pythonScriptPath
            });
        }
        
        // 检查数据目录是否存在
        const dataDir = '/home/hadoop/ml-latest-small';
        if (!fs.existsSync(dataDir)) {
            return res.status(500).json({
                status: 'error',
                message: 'Data directory not found',
                path: dataDir
            });
        }
        
        res.json({
            status: 'healthy',
            database: 'connected',
            python_script: 'found',
            data_directory: 'found'
        });
    });
});

/**
 * 测试Python脚本
 */
app.get('/recommendmovieforuser',function (req,res) {
    const userid=req.query.userid;
    const username=req.query.username;
    
    console.log('Starting recommendation for userid: ' + userid + ', username: ' + username);
    
    // Python脚本路径和参数
    const pythonScriptPath = '/home/hadoop/movie_recommendation_pyspark/movie_rec.py';
    const dataDir = '/home/hadoop/ml-latest-small';
    
    console.log('Running Python script: ' + pythonScriptPath);
    console.log('Data directory: ' + dataDir);
    console.log('User ID: ' + userid);
    
    // 调用Python推荐程序
    const pythonProcess = spawn('python3', [pythonScriptPath, dataDir, userid]);
    
    let output = '';
    let error = '';
    
    pythonProcess.stdout.on('data', (data) => {
        output += data.toString();
        console.log('Python output: ' + data.toString());
    });
    
    pythonProcess.stderr.on('data', (data) => {
        error += data.toString();
        console.error('Python error: ' + data.toString());
    });
    
    pythonProcess.on('close', (code) => {
        console.log('Python process exited with code ' + code);
        
        if (code !== 0) {
            console.error('Python script failed with error: ' + error);
            return res.status(500).send('推荐系统运行失败: ' + error);
        }
        
        console.log('Python script completed successfully');
        
        // 从数据库中读取推荐结果
        var selectRecommendResultSQL = `
            SELECT recommendresult.userid, recommendresult.movieid, 
                   recommendresult.rating, movieinfo.moviename, movieinfo.picture, movieinfo.movieurl 
            FROM recommendresult 
            INNER JOIN movieinfo ON recommendresult.movieid = movieinfo.movieid 
            WHERE recommendresult.userid = ${userid}
            ORDER BY recommendresult.rating DESC
            LIMIT 10`;
        
        connection.query(selectRecommendResultSQL, function(err, rows, fields) {
            if (err) {
                console.error('Database query error: ' + err);
                return res.status(500).send('数据库查询失败');
            }
            
            console.log('Read ' + rows.length + ' recommendations from database');
            
            var movieinfolist = [];
            for (var i = 0; i < rows.length; i++) {
                movieinfolist.push({
                    userid: rows[i].userid,
                    movieid: rows[i].movieid,
                    rating: rows[i].rating.toFixed(4), // 格式化评分
                    moviename: rows[i].moviename,
                    picture: rows[i].picture,
                    movieurl: rows[i].movieurl || '#'
                });
                console.log('Recommendation ' + (i+1) + ': ' + rows[i].moviename + ' (rating: ' + rows[i].rating + ')');
            }
            
            if (movieinfolist.length === 0) {
                console.log('No recommendations found for user ' + userid);
                movieinfolist.push({
                    userid: userid,
                    movieid: 0,
                    rating: 0,
                    moviename: '暂无推荐结果，请尝试评分更多电影',
                    picture: 'default.jpg'
                });
            }
            
            app.set('view engine', 'jade');
            res.render('recommendresult', {
                title: 'Recommend Result', 
                message: '为您推荐的电影', 
                username: username,
                userid: userid,
                movieinfo: movieinfolist
            });
            app.set('view engine', 'html');
        });
    });
    
    pythonProcess.on('error', (err) => {
        console.error('Failed to start Python process: ' + err);
        return res.status(500).send('无法启动推荐系统: ' + err.message);
    });
});

/**
 * 查看历史推荐结果（不重新计算）
 */
app.get('/viewrecommendations', function (req, res) {
    const userid = req.query.userid;
    const username = req.query.username;
    
    console.log('Viewing recommendations for user: ' + userid);
    
    var selectRecommendResultSQL = `
        SELECT recommendresult.userid, recommendresult.movieid, 
               recommendresult.rating, movieinfo.moviename, movieinfo.picture, movieinfo.movieurl 
        FROM recommendresult 
        INNER JOIN movieinfo ON recommendresult.movieid = movieinfo.movieid 
        WHERE recommendresult.userid = ${userid}
        ORDER BY recommendresult.rating DESC
        LIMIT 10`;
    
    connection.query(selectRecommendResultSQL, function(err, rows, fields) {
        if (err) {
            console.error('Database query error: ' + err);
            return res.status(500).send('数据库查询失败');
        }
        
        console.log('Found ' + rows.length + ' existing recommendations');
        
        var movieinfolist = [];
        for (var i = 0; i < rows.length; i++) {
            movieinfolist.push({
                userid: rows[i].userid,
                movieid: rows[i].movieid,
                rating: rows[i].rating.toFixed(4),
                moviename: rows[i].moviename,
                picture: rows[i].picture,
                movieurl: rows[i].movieurl || '#'
            });
        }
        
        if (movieinfolist.length === 0) {
            movieinfolist.push({
                userid: userid,
                movieid: 0,
                rating: 0,
                moviename: '暂无历史推荐，请先进行电影推荐',
                picture: 'default.jpg'
            });
        }
        
        app.set('view engine', 'jade');
        res.render('recommendresult', {
            title: 'Recommend Result', 
            message: '您的历史推荐', 
            username: username, 
            movieinfo: movieinfolist
        });
        app.set('view engine', 'html');
    });
});

/**
 * 清空用户推荐结果（用于测试）
 */
app.get('/clearrecommendations', function (req, res) {
    const userid = req.query.userid;
    
    if (!userid) {
        return res.status(400).send('用户ID不能为空');
    }
    
    connection.query('DELETE FROM recommendresult WHERE userid = ?', [userid], function(err, result) {
        if (err) {
            console.error('Error clearing recommendations: ' + err);
            return res.status(500).send('清除推荐结果失败');
        }
        
        console.log('Cleared recommendations for user ' + userid + ', affected rows: ' + result.affectedRows);
        res.send('已清除用户 ' + userid + ' 的推荐结果，影响行数: ' + result.affectedRows);
    });
});

/**
 * 查看用户评分记录
 */
app.get('/viewuserratings', function (req, res) {
    const userid = req.query.userid;
    const username = req.query.username;
    
    var selectRatingsSQL = `
        SELECT personalratings.userid, personalratings.movieid, 
               personalratings.rating, movieinfo.moviename, movieinfo.picture 
        FROM personalratings 
        INNER JOIN movieinfo ON personalratings.movieid = movieinfo.movieid 
        WHERE personalratings.userid = ${userid}
        ORDER BY personalratings.rating DESC`;
    
    connection.query(selectRatingsSQL, function(err, rows, fields) {
        if (err) {
            console.error('Database query error: ' + err);
            return res.status(500).send('数据库查询失败');
        }
        
        console.log('Found ' + rows.length + ' ratings for user ' + userid);
        
        var ratinglist = [];
        for (var i = 0; i < rows.length; i++) {
            ratinglist.push({
                userid: rows[i].userid,
                movieid: rows[i].movieid,
                rating: rows[i].rating,
                moviename: rows[i].moviename,
                picture: rows[i].picture
            });
        }
        
        app.set('view engine', 'jade');
        res.render('userratings', {
            title: 'User Ratings', 
            username: username, 
            ratings: ratinglist
        });
        app.set('view engine', 'html');
    });
});

/**
 * 获取电影列表API（首页展示用）
 */
app.get('/api/movies', function(req, res) {
    const limit = req.query.limit || 20;
    const offset = req.query.offset || 0;
    
    const selectMovieSQL = "select movieid, moviename, picture, movieurl from movieinfo limit " + parseInt(offset) + "," + parseInt(limit);
    
    connection.query(selectMovieSQL, function(err, rows, fields) {
        if (err) {
            res.json({ success: false, error: err.message });
            return;
        }
        res.json({ success: true, data: rows });
    });
});

/**
 * 获取推荐电影列表API（首页轮播下展示）
 */
app.get('/api/featured-movies', function(req, res) {
    const selectMovieSQL = "select movieid, moviename, picture, movieurl from movieinfo order by movieid desc limit 10";
    
    connection.query(selectMovieSQL, function(err, rows, fields) {
        if (err) {
            res.json({ success: false, error: err.message });
            return;
        }
        res.json({ success: true, data: rows });
    });
});

var  server=app.listen(3000,function () {
    console.log("movierecommend server start on port 3000......");
})