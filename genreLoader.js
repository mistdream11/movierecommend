const fs = require('fs');
const path = require('path');

let cachedGenres = [];
let sourcePath = '';

function parseGenres(csvPath) {
    const absolutePath = path.resolve(csvPath);
    sourcePath = absolutePath;

    if (!fs.existsSync(absolutePath)) {
        console.warn(`[GenreLoader] 未找到 movies.csv，路径: ${absolutePath}`);
        cachedGenres = [];
        return;
    }

    try {
        const content = fs.readFileSync(absolutePath, 'utf-8');
        const lines = content.split(/\r?\n/);
        const unique = new Set();

        for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            if (!line) {
                continue;
            }
            const lastCommaIndex = line.lastIndexOf(',');
            if (lastCommaIndex === -1) {
                continue;
            }
            const genresPart = line.slice(lastCommaIndex + 1).trim();
            if (!genresPart || genresPart === '(no genres listed)') {
                continue;
            }
            genresPart.split('|').forEach((genre) => {
                const normalized = genre.trim();
                if (normalized) {
                    unique.add(normalized);
                }
            });
        }

        cachedGenres = Array.from(unique).sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'));
        console.log(`[GenreLoader] 已从 ${absolutePath} 加载 ${cachedGenres.length} 个唯一类型标签`);
    } catch (err) {
        console.error('[GenreLoader] 解析 movies.csv 失败:', err);
        cachedGenres = [];
    }
}

function ensureLoaded(csvPath) {
    if (cachedGenres.length === 0 || (csvPath && path.resolve(csvPath) !== sourcePath)) {
        parseGenres(csvPath || sourcePath);
    }
}

function getGenres() {
    return cachedGenres;
}

function movieMatchesGenres(movie, selectedGenres) {
    if (!movie) {
        return false;
    }
    const genreField = movie.genre || movie.genres || '';
    if (!genreField) {
        return false;
    }
    return selectedGenres.some((genre) => genreField.includes(genre));
}

module.exports = {
    loadGenres: ensureLoaded,
    getGenres,
    movieMatchesGenres,
};
