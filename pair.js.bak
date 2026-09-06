
import express from 'express';
import fs from 'fs-extra';
import path from 'path';
import sharp from 'sharp';
import { exec } from 'child_process';
import mongoose from 'mongoose';
import moment from 'moment-timezone';
import https from 'https';
import axios from 'axios';
import dotenv from 'dotenv';
import yts from 'yt-search';
dotenv.config();

import {
    default as makeWASocket,
    useMultiFileAuthState,
    delay,
    Browsers,
    fetchLatestBaileysVersion,
    downloadContentFromMessage,
    jidNormalizedUser,
    isPnUser
} from '@whiskeysockets/baileys';

export const router = express.Router();
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const insecureAgent = new https.Agent({
    rejectUnauthorized: false
});
const config = {
    AUTO_RECORDING: 'false',
    AUTO_TYPING: 'false',
    AUTO_REACT: 'false',
    READ_CMD: 'false',
    API_MAIN_URL: 'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_MAIN_URL2:'https://api.laksidu.site',
    API_CINESUBZ_URL:'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_MOVIE_URL: 'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_KEY:'lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e',
    BOT_IMAGE:'https://cdn.phototourl.com/free/2026-09-05-74f20963-dcfb-4395-8249-f4b1152e918d.jpg',
    BOT_FOOTER:"SHAGGY XMD 〽️ᴏᴠɪᴇ Bᴏᴛ ᴠ1.1",
     MGROUP_LINK: 'https://chat.whatsapp.com/JpFSNrnqtnQIqdM0WlNds1',
    MOVIE_FOOTER:"​⏤͟͟͞͞★❮ SHAGGY XMD 〽️OVIE ⏤͟͟͞͞★",
     MOVIE_CAPTION:"🇸‌ʜᴀɢɢY-xᴍᴅ ᴍᴏᴠɪᴇ 🔥🌈",
    PREFIX: '.',
    OWNER_NUMBERS: ['94703830GGGG990'],
    BOT_NAME: "TEST-BOT",
    AIR_FOOTER: "ꜱʜᴀɢɢY-xᴍᴅ ᴠ1.2🎥",
    MODE: 'public',
    MAX_RETRIES: 3
};
const activeSockets = new Map();
const socketCreationTime = new Map();
const SESSION_BASE_PATH = './session';
const NUMBER_LIST_PATH = './numbers.json';
const SessionSchema = new mongoose.Schema({
    number: { type: String, unique: true, required: true },
    creds: { type: Object, required: true },
    config: { type: Object },
    updatedAt: { type: Date, default: Date.now }
});
const Session = mongoose.model('Session', SessionSchema);

async function connectMongoDB() {
    try {
        const mongoUri = process.env.MONGO_URI;
        await mongoose.connect(mongoUri, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        console.log(`
╔══════════════════════════════════════╗
║  ✅ MongoDB Connected Successfully   ║
║  ⚡ System Status : ONLINE           ║
╚══════════════════════════════════════╝
`);
    } catch (error) {
        console.error('MongoDB connection failed:', error);
        process.exit(1);
    }
}
connectMongoDB();
if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

function initialize() {
    activeSockets.clear();
    socketCreationTime.clear();
    console.log('Cleared active sockets and creation times on startup');
}
async function autoReconnectOnStartup() {
    try {
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            console.log(`Loaded ${(numbers.length)} numbers from numbers.json`);
        } else {
            console.warn('No numbers.json found, checking MongoDB for sessions...');
        }

        const sessions = await Session.find({}, 'number').lean();
        const mongoNumbers = sessions.map(s => s.number);
        console.log(`Found ${mongoNumbers.length} numbers in MongoDB sessions`);

        numbers = [...new Set([...numbers, ...mongoNumbers])];
        if (numbers.length === 0) {
            console.log('No numbers found in numbers.json or MongoDB, skipping auto-reconnect');
            return;
        }

        console.log(`Attempting to reconnect ${numbers.length} sessions...`);
        for (const number of numbers) {
            if (activeSockets.has(number)) {
                console.log(`Number ${number} already connected, skipping`);
                continue;
            }
            const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
            try {
                await EmpirePair(number, mockRes);
                console.log(`Initiated reconnect for ${number}`);
            } catch (error) {
                console.error(`Failed to reconnect ${number}:`, error);
            }
            await delay(1000);
        }
    } catch (error) {
        console.error('Auto-reconnect on startup failed:', error);
    }
}

initialize();
setTimeout(autoReconnectOnStartup, 5000);
function formatMessage(title, content, footer) {
    return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}
async function downloadContent(message) {
    if (!message) throw new Error('No message content');
    const buffer = await downloadContentFromMessage(message, 'buffer');
    return buffer;
}
async function streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}
async function setupCommandHandlers(socket, number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    let sessionConfig = await loadUserConfig(sanitizedNumber);
    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message) return;

        let text = '';
        if (msg.message.conversation) {
            text = msg.message.conversation.trim();
        } else if (msg.message.extendedTextMessage?.text) {
            text = msg.message.extendedTextMessage.text.trim();
        } else if (msg.message.buttonsResponseMessage) {
            text = msg.message.buttonsResponseMessage.selectedButtonId;
        } else {
            return;
        }

        const userJid = jidNormalizedUser(socket.user.id);
        const from = msg.key.remoteJid;
        const sender = from;
        const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
        const senderNumber = (nowsender || '').split('@')[0];
        const developers = `${config.OWNER_NUMBERS}`;
        const botNumber = socket.user.id.split(':')[0];
        const isbot = botNumber.includes(senderNumber);
        const isOwner = isbot ? isbot : developers.includes(senderNumber);
        const isGroup = from.endsWith("@g.us");
        const isCmd = text.startsWith(sessionConfig.PREFIX || '!');

        if (!sessionConfig.MODE === 'public') return;
        if (!isOwner && sessionConfig.MODE === 'private') return;
        if (!isOwner && isGroup && sessionConfig.MODE === 'inbox') return;
        if (!isOwner && !isGroup && sessionConfig.MODE === 'groups') return;

        if (isCmd && sessionConfig.READ_CMD === 'true') {
            try {
                await socket.readMessages([msg.key]);
            } catch (error) {
               
            }
        }

        if (!isCmd) return;
        const parts = text.slice((sessionConfig.PREFIX || '!').length).trim().split(/\s+/);
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);

        const groupMetadata = isGroup ? await socket.groupMetadata(msg.key.remoteJid) : {};
        const participants = groupMetadata.participants || [];
        const groupAdmins = participants.filter((p) => p.admin).map((p) => p.id);
        const isBotAdmins = groupAdmins.includes(socket.user.id);
        const isAdmins = groupAdmins.includes(sender);

        const reply = async (text, options = {}) => {
            await socket.sendMessage(msg.key.remoteJid, { text, ...options }, { quoted: msg });
        };

        try {
            switch (command) {
            case 'song':
    if (!args.length) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n*Need YouTube URL or Song Title*'
        }, { quoted: msg });
        break;
    }

    const songQuery = args.join(' ');
    await socket.sendMessage(sender, { text: '🔍 Searching song...' });

    try {
        let data;
        if (songQuery.match(/(youtube\.com|youtu\.be)/)) {
            const match = songQuery.match(/(?:v=|\/)([0-9A-Za-z_-]{11})/);
            const videoId = match ? match[1] : null;

            if (!videoId) throw new Error('Invalid YouTube URL');

            const result = await yts({ videoId });
            data = result;
        } else {
            const result = await yts(songQuery);

            if (!result.videos || result.videos.length === 0) {
                await socket.sendMessage(sender, {
                    text: '❌ NO RESULTS\n\n*No results found for your query*'
                }, { quoted: msg });
                break;
            }

            data = result.videos[0];
        }

        if (!data) throw new Error('No results');

        const videoId = data.videoId;
        const desc = ` *ᴛɪᴛʟᴇ* : _${data.title || 'N/A'}_     

* ⏱️ 𝗗ᴜʀᴀᴛɪᴏɴ* ➟ _${data.timestamp || 'N/A'}_
* 👀 𝗩ɪᴇᴡꜱ* ➟ _${data.views?.toLocaleString() || 'N/A'}_
* 📅 𝗣ᴜʙʟɪꜱʜᴇᴅ* ➟ _${data.ago || 'N/A'}_
* 🎤 𝗖ʜᴀɴɴᴇʟ* ➟ _${data.author?.name || 'N/A'}_
*🔢 𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 👇*

*01 ᴅᴏᴡɴʟᴏᴀᴅ ᴀᴜᴅɪᴏ 🌐*
*02 ᴅᴏᴡɴʟᴏᴀᴅ ᴅᴏᴄᴜᴍᴇɴᴛ 🌐*
`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: data.thumbnail },
            caption: desc
        }, { quoted: msg });
        const listener = async (update) => {
            const mek = update.messages[0];
            if (!mek?.message) return;
            const ctx = mek.message.extendedTextMessage?.contextInfo;
            if (!ctx || ctx.stanzaId !== sentMsg.key.id) return;
            const text =
                mek.message.conversation ||
                mek.message.extendedTextMessage?.text;

            if (!['1', '2'].includes(text)) return;
            socket.ev.off('messages.upsert', listener);

            await socket.sendMessage(sender, { react: { text: '⬇️', key: mek.key } });

            try {
                 const apiUrl = `${config.API_MAIN_URL}/api/ytmp3?url=https://youtu.be/${videoId}&api_key=${config.API_KEY}`;
                const res = await axios.get(apiUrl, { timeout: 20000 });

                if (res.data.status !== 'success') {
                    throw new Error(res.data.message || 'API Error');
                }
                const downloadLink = res.data.data.download_url;
                const songTitle = res.data.data.title || data.title;
                const thumbnail = res.data.data.thumbnail || data.thumbnail;
                await socket.sendMessage(sender, { react: { text: '⬆️', key: mek.key } });
                const fileName = songTitle.replace(/[^a-zA-Z0-9]/g, '_');
                if (text === '1') {
                    await socket.sendMessage(sender, {
                        audio: { url: downloadLink },
                        mimetype: 'audio/mpeg'
                    }, { quoted: mek });
                } else if (text === '2') {
                    await socket.sendMessage(sender, {
                        document: { url: downloadLink },
                        mimetype: 'audio/mpeg',
                        fileName: `${fileName}.mp3`,
                        caption: songTitle
                    }, { quoted: mek });
                }

                await socket.sendMessage(sender, { react: { text: '✅', key: mek.key } });

            } catch (err) {
                await socket.sendMessage(sender, {
                    text: '❌ DOWNLOAD ERROR\n\n' + err.message
                }, { quoted: mek });

                await socket.sendMessage(sender, { react: { text: '❌', key: mek.key } });
            }
        };

        socket.ev.on('messages.upsert', listener);
        setTimeout(() => {
            socket.ev.off('messages.upsert', listener);
        }, 300000);

    } catch (err) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n' + err.message
        }, { quoted: msg });
    }

    break;  
                 case 'tiktok':
    if (!args.length || !args.join(' ').startsWith('https://')) {
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Please provide a valid TikTok URL!

📋 Example: .tiktok  https://www.tiktok.com/@user/video/xyz`
        });
        break;
    }

    await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

    let tiktokTimeout;

    try {
        const tiktokUrl = args.join(' ');
        const response = await axios.get(`${config.API_MAIN_URL}/tiktok/download?url=${encodeURIComponent(tiktokUrl)}&api_key=${config.API_KEY}`);
        const tiktokData = response.data.result;

        if (!response.data.status || !tiktokData) {
            await socket.sendMessage(sender, {
                image: { url: config.ERROR },
                caption: `❌ ERROR

Failed to fetch TikTok video! Please try again later.`
            });
            break;
        }

        const captionMessage = `☘️ *TIKTOK DOWNLOADER*

📝 Title: ${tiktokData.title || 'TikTok Video'}
👤 Author: ${tiktokData.author?.nickname || 'Unknown'}
❤️ Likes: ${tiktokData.digg_count?.toLocaleString() || 'N/A'}
👀 Views: ${tiktokData.play_count?.toLocaleString() || 'N/A'}
💬 Comments: ${tiktokData.comment_count?.toLocaleString() || 'N/A'}
⏱️ Duration: ${tiktokData.duration || 'N/A'} seconds

⬇️ DOWNLOAD OPTIONS

🔢 Reply with a number:

*1 ║❯❯ No Watermark ☊*
*2 ║❯❯ With Watermark ☊*
*3 ║❯❯ Audio Only ☊*`;

        const sentMessage = await socket.sendMessage(sender, {
            image: { url: tiktokData.cover || config.SITHIJA_IMAGE_PATH },
            caption: captionMessage
        }, { quoted: msg });

        const messageID = sentMessage.key.id;

        const handleTikTokSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const userResponse = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                if (tiktokTimeout) clearTimeout(tiktokTimeout);
                
                await socket.sendMessage(sender, { react: { text: '⬇️', key: replyMek.key } });

                const downloadLinks = tiktokData.downloads;
                let mediaMessage;

                try {
                    switch (userResponse) {
                        case '1':
                            mediaMessage = {
                                video: { url: downloadLinks.no_watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

No Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '2':
                            mediaMessage = {
                                video: { url: downloadLinks.watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

With Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '3':
                            mediaMessage = {
                                audio: { url: downloadLinks.audio },
                                mimetype: 'audio/mpeg',
                                caption: `✅ TIKTOK AUDIO

Audio Only
📝 ${tiktokData.title}`
                            };
                            break;

                        default:
                            await socket.sendMessage(sender, {
                                image: { url: config.ERROR },
                                caption: `❌ INVALID SELECTION

Please reply with 1, 2, 3, or 4.`
                            });
                            return;
                    }

                    await socket.sendMessage(sender, mediaMessage, { quoted: replyMek });
                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (sendError) {
                    console.error('TikTok send error:', sendError);
                    await socket.sendMessage(sender, {
                        image: { url: config.ERROR },
                        caption: `❌ ERROR

Failed to send: ${sendError.message}`
                    }, { quoted: replyMek });
                } finally {
                    socket.ev.off('messages.upsert', handleTikTokSelection);
                }
            }
        };

        socket.ev.on('messages.upsert', handleTikTokSelection);

        tiktokTimeout = setTimeout(() => {
            socket.ev.off('messages.upsert', handleTikTokSelection);
            console.log('TikTok selection timeout - cleaned up');
        }, 120000);

    } catch (error) {
        console.error('TikTok download error:', error);
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Failed to process TikTok request: ${error.message}`
        });
    }
    break;
case 'cinesubz':
    if (!args.length) {
        await socket.sendMessage(sender, {
            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ හෝ TV series එකේ නම ලබාදෙන්න! උදා: .cinesubz batman*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const cinezubQuerytv = args.join(' ');
    await socket.sendMessage(sender, { text: '📽️ 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙤𝙣 𝘾𝙞𝙣𝙚𝙨𝙪𝙗𝙯...' });

    try {
        const searchResponse = await axios.get(`https://apis.laksidu.site/cinesubz/search?query=${encodeURIComponent(cinezubQuerytv)}&api_key=lakiyaofc2`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.results || searchData.results.length === 0) {
            await socket.sendMessage(sender, {
                image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*cinesubz හි චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cinezubResults = searchData.results.slice(0, 25);
        let listText = `☘️ *𝗠𝗢𝗩𝗜𝗘 : _𝗦𝗘𝗔𝗥𝗖𝗛 𝗥𝗘𝗦𝗨𝗟𝗧𝗦_* 🔍
╭──────●➤
🔎 *𝗤𝘂𝗲𝗿𝘆 ➟* _${cinezubQuerytv}_
📊 *Status ➟* _Results Found_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*╭──────●➤*\n\n`;

        cinezubResults.forEach((item, index) => {
            const type = item.link.includes('/tvshows/') ? '📺 TV Series' : '🎬 Movie';
            listText += `*🍟${index + 1} ║❯❯ ${type} | ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;
        
        const sentMsg = await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cinezubResults.length) {
                    await socket.sendMessage(sender, {
                        image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${cinezubResults.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = cinezubResults[choice];
                const isTvShow = selectedItem.link.includes('/tvshows/');
                
                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: '📺 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙏𝙑 𝙨𝙚𝙧𝙞𝙚𝙨 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`https://apis.laksidu.site/cinesubz/tvshow?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;
                        
                        // 🟢 NEW STRUCTURE - Extract data correctly
                        const rating = tvInfo.rating?.score || 'N/A';
                        const totalEpisodes = tvInfo.episodes?.total || 'N/A';
                        const episodesList = tvInfo.episodes?.list || [];
                        
                        // Group episodes by season (extract season from episode title or URL)
                        const seasonsMap = {};
                        episodesList.forEach(ep => {
                            let seasonNum = '1';
                            // Try to extract season from episode number or title
                            const seasonMatch = ep.number?.match(/^(\d+)/);
                            if (seasonMatch) {
                                seasonNum = seasonMatch[1];
                            }
                            if (!seasonsMap[seasonNum]) {
                                seasonsMap[seasonNum] = [];
                            }
                            seasonsMap[seasonNum].push(ep);
                        });
                        
                        const seasonsArray = Object.keys(seasonsMap).map(season => ({
                            season: parseInt(season),
                            total_episodes: seasonsMap[season].length,
                            episodes: seasonsMap[season].map(ep => ({
                                episode: ep.number || '1',
                                title: ep.title || 'Episode',
                                url: ep.url || ''
                            }))
                        }));
                        
                        const totalSeasons = seasonsArray.length;
                        
                        let tvDetailsText = 
    `☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟*  _${rating}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟*_${tvInfo.year || 'N/A'}_
▫️📀 *𝗦ᴇᴀꜱᴏɴꜱ ➟* _${totalSeasons} Total_
▫️📊 *𝗘ᴘɪꜱᴏᴅᴇꜱ ➟* _${totalEpisodes} Total_
*➟➟➟➟➟➟➟➟➟➟*
📖 *𝗦𝗧𝗢𝗥𝗬*_${tvInfo.description?.substring(0, 30) || 'No description available.'}..._`;

                        await socket.sendMessage(sender, {
                            image: { url: tvInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        let seasonsText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗦𝗘𝗔𝗦𝗢𝗡 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
*➟➟➟➟➟➟➟➟➟➟*
⬇️🍀 *𝗦𝗘𝗟𝗘𝗖𝗧 𝗬𝗢𝗨𝗥 𝗦𝗘𝗔𝗦𝗢𝗡*
*➟➟➟➟➟➟➟➟➟➟*
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦ᴇʟᴇᴄ𝘛*
*➟➟➟➟➟➟➟➟➟➟*\n\n`;

                        seasonsArray.forEach((season, idx) => {
                            seasonsText += `🍀 *${idx + 1} ┃》📀 Season ${season.season} (${season.total_episodes} episodes)*\n`;
                        });

                        seasonsText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                        const seasonMsg = await socket.sendMessage(sender, {
                            text: seasonsText
                        }, { quoted: replyMek });

                        const seasonMsgID = seasonMsg.key.id;

                        const handleSeasonSelect = async ({ messages: seasonMessages }) => {
                            const seasonMek = seasonMessages[0];
                            if (!seasonMek?.message) return;

                            const seasonChoice = seasonMek.message.conversation || seasonMek.message.extendedTextMessage?.text;
                            const isReplyToSeasonMsg = seasonMek.message.extendedTextMessage?.contextInfo?.stanzaId === seasonMsgID;

                            if (isReplyToSeasonMsg && sender === seasonMek.key.remoteJid) {
                                const seasonNum = parseInt(seasonChoice) - 1;
                                
                                if (isNaN(seasonNum) || seasonNum < 0 || seasonNum >= seasonsArray.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${seasonsArray.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: seasonMek });
                                    return;
                                }

                                const selectedSeason = seasonsArray[seasonNum];
                                
                                let episodesText =
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗘𝗣𝗜𝗦𝗢𝗗𝗘 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡_* 📺
╭──────●➤
☘️ *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📊 *𝗧ᴏᴛᴀʟ ➟* _${selectedSeason.total_episodes} Episodes_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                selectedSeason.episodes.forEach((ep, idx) => {
                                    episodesText += `*⭐${idx + 1} ║❯❯ 📺 Episode ${ep.episode}: ${ep.title}*\n`;
                                });

                                episodesText += `\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

                                const episodeMsg = await socket.sendMessage(sender, {
                                    text: episodesText
                                }, { quoted: seasonMek });

                                const episodeMsgID = episodeMsg.key.id;

                                const handleEpisodeSelect = async ({ messages: episodeMessages }) => {
                                    const episodeMek = episodeMessages[0];
                                    if (!episodeMek?.message) return;

                                    const episodeChoice = episodeMek.message.conversation || episodeMek.message.extendedTextMessage?.text;
                                    const isReplyToEpisodeMsg = episodeMek.message.extendedTextMessage?.contextInfo?.stanzaId === episodeMsgID;

                                    if (isReplyToEpisodeMsg && sender === episodeMek.key.remoteJid) {
                                        const choiceNum = parseInt(episodeChoice);
                                        
                                        if (isNaN(choiceNum) || choiceNum < 1 || choiceNum > selectedSeason.episodes.length) {
                                            await socket.sendMessage(sender, {
                                                image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                                caption: formatMessage(
                                                    '❌ INVALID SELECTION',
                                                    `*වැරදි අංකයක්! 1-${selectedSeason.episodes.length} අතර තෝරන්න!*`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            return;
                                        }

                                        const selectedEpisode = selectedSeason.episodes[choiceNum - 1];
                                        
                                        await socket.sendMessage(sender, { 
                                            text: `📥 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠𝙨 𝙛𝙤𝙧 S${selectedSeason.season}E${selectedEpisode.episode}...` 
                                        }, { quoted: episodeMek });

                                        try {
                                            // 🟢 NEW: Episode API URL
                                            const episodeResponse = await axios.get(`https://apis.laksidu.site/api/episode?url=${encodeURIComponent(selectedEpisode.url)}&api_key=lakiyaofc2`);
                                            const episodeData = episodeResponse.data;

                                            if (!episodeData.status || !episodeData.data?.download_links?.length) {
                                                throw new Error('Failed to get episode download links');
                                            }

                                            const episodeDownloadLinks = episodeData.data.download_links;
                                            
                                            let qualityText = 
    `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦_* 📺
╭──────●➤
🎬 *𝗧ɪᴛʟᴇ ➟* _${tvInfo.title || 'N/A'}_
📀 *𝗦ᴇᴀꜱᴏɴ ➟* _Season ${selectedSeason.season}_
📺 *𝗘ᴘɪꜱᴏᴅᴇ ➟* _${selectedEpisode.episode} : ${selectedEpisode.title}_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤\n\n`;

                                            episodeDownloadLinks.forEach((link, idx) => {
                                                const quality = link.meta || link.type || `Quality ${idx + 1}`;
                                                qualityText += `🔥 *${idx + 1} ║❯❯ 📥 ${quality}*\n`;
                                            });

                                            qualityText += `\n${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                                            const qualityMsg = await socket.sendMessage(sender, {
                                                text: qualityText
                                            }, { quoted: episodeMek });

                                            const qualityMsgID = qualityMsg.key.id;

                                            const handleQualitySelect = async ({ messages: qualityMessages }) => {
                                                const qualityMek = qualityMessages[0];
                                                if (!qualityMek?.message) return;

                                                const qualityChoice = qualityMek.message.conversation || qualityMek.message.extendedTextMessage?.text;
                                                const isReplyToQualityMsg = qualityMek.message.extendedTextMessage?.contextInfo?.stanzaId === qualityMsgID;

                                                if (isReplyToQualityMsg && sender === qualityMek.key.remoteJid) {
                                                    const qualityNum = parseInt(qualityChoice) - 1;
                                                    
                                                    if (isNaN(qualityNum) || qualityNum < 0 || qualityNum >= episodeDownloadLinks.length) {
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                                            caption: formatMessage(
                                                                '❌ INVALID SELECTION',
                                                                `*වැරදි අංකයක්! 1-${episodeDownloadLinks.length} අතර තෝරන්න!*`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                        return;
                                                    }

                                                    const selectedQuality = episodeDownloadLinks[qualityNum];
                                                    
                                                    await socket.sendMessage(sender, { 
                                                        text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                                    }, { quoted: qualityMek });

                                                    try {
                                                        // 🟢 NEW: Download API - using selectedQuality.url (full ZT link)
                                                        const downloadApiUrl = `https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedQuality.url)}&api_key=lakiyaofc2`;
                                                        const darkShanResponse = await axios.get(downloadApiUrl);
                                                        const darkShanData = darkShanResponse.data;

                                                        if (!darkShanData.status || !darkShanData.data?.download) {
                                                            throw new Error('Failed to get download URL');
                                                        }

                                                        const finalDownloadLinks = darkShanData.data.download;
                                                        
                                                        const finalNonTelegramLinks = finalDownloadLinks.filter(link => 
                                                            link.name && link.name.toLowerCase() !== 'telegram'
                                                        );
                                                        
                                                        if (finalNonTelegramLinks.length === 0) {
                                                            throw new Error('No non-Telegram download links available');
                                                        }
                                                        
                                                        const finalLink = finalNonTelegramLinks.find(link => link.name === 'unknown') || finalNonTelegramLinks[0];
                                                        
                                                        await socket.sendMessage(sender, { react: { text: '📥', key: qualityMek.key } });

                                                        await socket.sendMessage(sender, {
                                                            document: { url: finalLink.url },
                                                            mimetype: 'video/mp4',
                                                            fileName: `${tvInfo.title || 'Series'} S${selectedSeason.season}E${selectedEpisode.episode} - ${selectedEpisode.title}.mp4`,
                                                            caption: `*☘️ ${tvInfo.title || 'Series'} - ${selectedSeason.season}*

\`[Episode-${selectedEpisode.episode}]\`

${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                                        }, { quoted: qualityMek });

                                                        await socket.sendMessage(sender, { react: { text: '✅', key: qualityMek.key } });

                                                    } catch (downloadError) {
                                                        console.error('Download error:', downloadError);
                                                        await socket.sendMessage(sender, {
                                                            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                                            caption: formatMessage(
                                                                '❌ DOWNLOAD ERROR',
                                                                `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                            )
                                                        }, { quoted: qualityMek });
                                                    } finally {
                                                        socket.ev.off('messages.upsert', handleQualitySelect);
                                                        socket.ev.off('messages.upsert', handleEpisodeSelect);
                                                        socket.ev.off('messages.upsert', handleSeasonSelect);
                                                        socket.ev.off('messages.upsert', handleSelection);
                                                    }
                                                }
                                            };

                                            socket.ev.on('messages.upsert', handleQualitySelect);

                                        } catch (error) {
                                            console.error('Error fetching episode links:', error);
                                            await socket.sendMessage(sender, {
                                                image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                                caption: formatMessage(
                                                    '❌ ERROR',
                                                    `*Download links ලබාගැනීමේ දෝෂයක්*\n${error.message}`,
                                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                                )
                                            }, { quoted: episodeMek });
                                            socket.ev.off('messages.upsert', handleEpisodeSelect);
                                            socket.ev.off('messages.upsert', handleSeasonSelect);
                                            socket.ev.off('messages.upsert', handleSelection);
                                        }
                                    }
                                };

                                socket.ev.on('messages.upsert', handleEpisodeSelect);
                            }
                        };

                        socket.ev.on('messages.upsert', handleSeasonSelect);

                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*TV series details ලබාගැනීමේ දෝෂයක්*\n${tvShowError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                    
                } else {
                    await socket.sendMessage(sender, { 
                        text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`https://apis.laksidu.site/cinesubz/details?url=${encodeURIComponent(selectedItem.link)}&api_key=lakiyaofc2`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;
                        
                        const validDownloads = movieInfo.downloads?.filter(dl => dl && dl.quality && dl.url) || [];
                        
                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                caption: formatMessage(
                                    '❌ NO DOWNLOADS',
                                    '*මෙම චිත්‍රපටය සඳහා බාගත කිරීමේ link නොමැත!*',
                                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                )
                            }, { quoted: replyMek });
                            return;
                        }
                        
                        const description = movieInfo.description?.substring(0, 300) + (movieInfo.description?.length > 300 ? '...' : '') || 'No description available.';
                        
                        const imdbRating = movieInfo.imdb_rating ? `${movieInfo.imdb_rating}/10` : 'N/A';
                        const year = movieInfo.year || 'N/A';
                        const runtime = movieInfo.runtime || 'N/A';
                        const director = movieInfo.director || 'N/A';
                        const country = movieInfo.country || 'N/A';
                        const cast = Array.isArray(movieInfo.cast) ? movieInfo.cast.join(', ') : movieInfo.cast || 'N/A';
                        
                        const movieDetailsCaption = formatMessage(
                            `☘️ *𝗧ɪᴛʟᴇ ➟* _${movieInfo.title}_`,
                            `▫️🥇 *𝗜𝗺𝗱𝗯 𝗥ᴀᴛɪɴɢ ➟* _${imdbRating}_
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟* _${runtime}_
▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟* _${year}_
▫️🎬 *𝗗ɪʀᴇᴄᴛᴏʀ ➟* _${director}_
▫️🌎 *𝗖ᴏᴜɴᴛʀʏ ➟* _${country}_
▫️👥 *𝗖ᴀꜱᴛ ➟* _${cast}_
*➟➟➟➟➟➟➟➟➟➟*
*📖 𝗦𝗧𝗢𝗥𝗬 ➟*_${description}_`,
                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                        );

                        await socket.sendMessage(sender, {
                            image: { url: movieInfo.poster || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                            caption: movieDetailsCaption
                        }, { quoted: replyMek });

                        const downloadOptionsCaption = formatMessage(
                            `⬇️🍀 *𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*`,
                            `${validDownloads.map((dl, i) => `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${dl.quality}*`).join('\n')}\n

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        );

                        const downloadOptionsMsg = await socket.sendMessage(sender, {
                            text: downloadOptionsCaption
                        }, { quoted: replyMek });

                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;
                                
                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                        caption: formatMessage(
                                            '❌ INVALID SELECTION',
                                            `*වැරදි අංකයක්! 1-${validDownloads.length} අතර තෝරන්න!*`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];
                                
                                await socket.sendMessage(sender, { 
                                    text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                                }, { quoted: downloadMek });

                                try {
                                    const downloadResponse = await axios.get(`https://apis.laksidu.site/dl/cinesubz?url=${encodeURIComponent(selectedDownload.url)}&api_key=lakiyaofc2`);
                                    const downloadData = downloadResponse.data;

                                    if (!downloadData.status || !downloadData.data?.download) {
                                        throw new Error('Failed to get download URL');
                                    }

                                    const downloadLinks = downloadData.data.download;
                                    
                                    const nonTelegramLinks = downloadLinks.filter(link => 
                                        link.name && link.name.toLowerCase() !== 'telegram'
                                    );
                                    
                                    if (nonTelegramLinks.length === 0) {
                                        throw new Error('No non-Telegram download links available');
                                    }
                                    
                                    const preferredLink = nonTelegramLinks.find(link => link.name === 'unknown') || nonTelegramLinks[0];
                                    
                                    await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                    await socket.sendMessage(sender, {
                                        document: { url: preferredLink.url },
                                        mimetype: 'video/mp4',
                                        fileName: downloadData.data.title || `${movieInfo.title} ${selectedDownload.quality}.mp4`,
                                        caption: formatMessage(
                                            `☘️ ${movieInfo.title}`,
                                            `\`❚█═${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}═█❚\`
                                            
\`[WEB-DL-${selectedDownload.quality}]\``,
                                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                        caption: formatMessage(
                                            '❌ DOWNLOAD ERROR',
                                            `*Download link එක ලබාගැනීමේ දෝෂයක්.*\n${downloadError.message}`,
                                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                        )
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                            caption: formatMessage(
                                '❌ ERROR',
                                `*Details ලබාගැනීමේ දෝෂයක්*\n${detailsError.message}`,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Cinezub command error:', error);
        await socket.sendMessage(sender, {
            image:  { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    
    break;
                case 'sinhalasub':
    if (!args.length) {
        await socket.sendMessage(sender, {
             image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .sinhalasub spider*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const movieQuery55 = args.join(' ');
   
    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

 
    let sinhalasubSelectionListener = null;
    let sinhalasubDownloadListener = null;
    let sinhalasubSelectionTimeout = null;
    let sinhalasubDownloadTimeout = null;
    
 
    let sinhalasubMasterTimeout = null;
    const clearAllSinhalasubListeners = () => {
        console.log('🧹 Clearing all Sinhalasub listeners');
        
       
        if (sinhalasubSelectionListener) {
            socket.ev.off('messages.upsert', sinhalasubSelectionListener);
            sinhalasubSelectionListener = null;
        }
        if (sinhalasubSelectionTimeout) {
            clearTimeout(sinhalasubSelectionTimeout);
            sinhalasubSelectionTimeout = null;
        }
        
     
        if (sinhalasubDownloadListener) {
            socket.ev.off('messages.upsert', sinhalasubDownloadListener);
            sinhalasubDownloadListener = null;
        }
        if (sinhalasubDownloadTimeout) {
            clearTimeout(sinhalasubDownloadTimeout);
            sinhalasubDownloadTimeout = null;
        }
        
       
        if (sinhalasubMasterTimeout) {
            clearTimeout(sinhalasubMasterTimeout);
            sinhalasubMasterTimeout = null;
        }
    };

    try {
        const searchResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/search?query=${encodeURIComponent(movieQuery55)}&api_key=${config.API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data?.results || searchData.data.results.length === 0) {
            await socket.sendMessage(sender, {
                 image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*චිත්‍රපට හමුවෙන්නේ නැත! 😞*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movies = searchData.data.results.slice(0, 115);
        let listText = `🎀 *𝗦𝗘𝗔𝗥𝗖𝗛 : _${movieQuery55}_*
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
╭──────●➤\n`;

        movies.forEach((movie, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${movie.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

      
        sinhalasubMasterTimeout = setTimeout(() => {
            clearAllSinhalasubListeners();
            console.log('🧹 Sinhalasub master timeout - All listeners cleared after 3 minutes');
        }, 180000);

       
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
               
                if (sinhalasubSelectionTimeout) {
                    clearTimeout(sinhalasubSelectionTimeout);
                    sinhalasubSelectionTimeout = null;
                }
                
               
                sinhalasubSelectionTimeout = setTimeout(() => {
                    if (sinhalasubSelectionListener) {
                        socket.ev.off('messages.upsert', sinhalasubSelectionListener);
                        sinhalasubSelectionListener = null;
                        console.log('🧹 Sinhalasub selection listener timeout');
                    }
                    sinhalasubSelectionTimeout = null;
                }, 120000);

                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movies.length) {
                    await socket.sendMessage(sender, {
                         image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                        caption: formatMessage(
                            '❌ INVALID SELECTION',
                            `*වැරදි අංකයක්! 1-${movies.length} අතර තෝරන්න! 😕*`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                    return;
                }

                const selectedMovie = movies[choice];
                
                await socket.sendMessage(sender, { 
                    text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                }, { quoted: replyMek });

                
                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                try {
                    const infoResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/info?url=${encodeURIComponent(selectedMovie.url)}&api_key=${config.API_KEY}`);
                    const infoData = infoResponse.data;

                    if (!infoData.status || !infoData.data) {
                        throw new Error('Failed to fetch movie details');
                    }

                    const movieInfo = infoData.data.movie;
                    const downloads = infoData.data.downloads || [];

                    
                    const videoDownloads = downloads.filter(d => d.server === 'pixeldrain');

                    if (videoDownloads.length === 0) {
                        await socket.sendMessage(sender, {
                             image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                            caption: formatMessage(
                                '❌ NO DOWNLOADS',
                                '*Pixeldrain බාගත කිරීම් නොමැත!*',
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: replyMek });
                        return;
                    }

                    const castPreview = movieInfo.cast?.slice(0, 5).join(', ') + (movieInfo.cast?.length > 5 ? '...' : '');
                    
                    const detailsCaption = formatMessage(
                        `🍀 *𝗧ɪᴛʟᴇ : ${movieInfo.title}`,
                        `▫️📅 *𝗥ᴇʟᴇᴀꜱᴇ 𝗬ᴇᴀʀ ➟ ${movieInfo.year || 'N/A'}*
▫️🥇 *𝗜𝗺𝗱ʙ 𝗥ᴀᴛɪɴɢ ➟ ${movieInfo.rating || 'N/A'}/10*
▫️📊 *𝗤ᴜᴀʟɪᴛʏ ➟ ${movieInfo.quality || 'N/A'}*
▫️⏳ *𝗗ᴜʀᴀᴛɪᴏɴ ➟ ${movieInfo.runtime || 'N/A'}*
▫️🔠 *𝗟ᴀɴɢᴜᴀɢᴇ ➟ ${movieInfo.language || 'N/A'}*
▫️🎭 *𝗚ᴇɴʀᴇꜱ ➟ ${movieInfo.genres?.join(', ') || 'N/A'}*
▫️🙅 *𝗗ɪʀᴇᴄᴛᴏʀ ➟ ${movieInfo.director?.slice(0,2).join(', ') || 'N/A'}*
▫️👥 *𝗖ᴀꜱᴛ ➟ ${castPreview || 'N/A'}*
▫️👨‍💻 *𝗦ᴜʙᴛɪᴛʟᴇ ➟ ${movieInfo.subtitle?.author || 'Sinhala'} (${movieInfo.subtitle?.site || 'Baiscope'})*
▫️📖 *sᴛᴏʀʏ ➟ ${movieInfo.description?.substring(0, 150) || 'No description'}...*
▫️🔗 *Jᴏɪɴ ➟ ${sessionConfig.MGROUP_LINK || config.MGROUP_LINK}*`,
                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                    );

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: movieInfo.poster || selectedMovie.poster || sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: detailsCaption
                    }, { quoted: replyMek });

                    
                    await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                    const downloadOptionsText = `*⬇️🎀 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*
*Reply with number 👇*

${videoDownloads.map((d, i) => 
`*🔰 ${i + 1} ┃ 📥 ${d.quality || 'N/A'} • ${d.size || 'N/A'}*`
).join('\n')}

${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

                    const downloadMsg = await socket.sendMessage(sender, {
                        text: downloadOptionsText
                    }, { quoted: infoMsg });

                    const infoMsgID = downloadMsg.key.id;

                   
                    const handleDownload = async ({ messages: downloadMessages }) => {
                        const downloadMek = downloadMessages[0];
                        if (!downloadMek?.message) return;

                        const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                        const isReplyToInfoMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isReplyToInfoMsg && sender === downloadMek.key.remoteJid) {
                         
                            if (sinhalasubDownloadTimeout) {
                                clearTimeout(sinhalasubDownloadTimeout);
                                sinhalasubDownloadTimeout = null;
                            }
                            
                          
                            sinhalasubDownloadTimeout = setTimeout(() => {
                                if (sinhalasubDownloadListener) {
                                    socket.ev.off('messages.upsert', sinhalasubDownloadListener);
                                    sinhalasubDownloadListener = null;
                                    console.log('🧹 Sinhalasub download listener timeout');
                                }
                                sinhalasubDownloadTimeout = null;
                            }, 120000);

                            const choiceNum = parseInt(downloadChoice) - 1;
                            
                            if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= videoDownloads.length) {
                                await socket.sendMessage(sender, {
                                     image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                                    caption: formatMessage(
                                        '❌ INVALID SELECTION',
                                        `*වැරදි අංකයක්! 1-${videoDownloads.length} අතර තෝරන්න!*`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                                return;
                            }

                            const selectedDownload = videoDownloads[choiceNum];
                            
                            await socket.sendMessage(sender, { 
                                text: `⏳ 𝙂𝙚𝙩𝙩𝙞𝙣𝙜 𝙙𝙤𝙬𝙣𝙡𝙤𝙖𝙙 𝙡𝙞𝙣𝙠...` 
                            }, { quoted: downloadMek });

                          
                            await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 2000));

                            try {
                               
                                const downloadResponse = await axios.get(`${config.API_MAIN_URL}/sinhalasub/download2?url=${encodeURIComponent(selectedDownload.link_page)}&api_key=${config.API_KEY}`);
                                const downloadData = downloadResponse.data;

                                if (!downloadData.status || !downloadData.data?.download) {
                                    throw new Error('Failed to get download URL');
                                }

                                const finalDownloadUrl = downloadData.data.download;
                                const fileInfo = downloadData.data.file_info || {};
                                
                                
                                let fileName = fileInfo.name || `${movieInfo.title} [${selectedDownload.quality || 'Unknown'}].mp4`;
                                const mimeType = fileInfo.mimeType || 'video/mp4';
                                
                                console.log('Download URL:', finalDownloadUrl);
                                console.log('File Name:', fileName);
                                console.log('Mime Type:', mimeType);
                                
                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                             

                               
                                let sizeText = 'N/A';
                                if (fileInfo.size) {
                                    const sizeInMB = fileInfo.size / 1024 / 1024;
                                    if (sizeInMB > 1024) {
                                        sizeText = (sizeInMB / 1024).toFixed(2) + ' GB';
                                    } else {
                                        sizeText = sizeInMB.toFixed(2) + ' MB';
                                    }
                                }

                               
                                await socket.sendMessage(sender, {
                                    document: { url: finalDownloadUrl },
                                    mimetype: mimeType,
                                    fileName: fileName,
                                    caption: formatMessage(
                                        `🍀 ${movieInfo.title}`,
                                        `\`❚█${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}█❚\`

\`❪${selectedDownload.quality || 'Unknown'}❫\``,
                                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                                    )
                                }, { quoted: downloadMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });
                                
                               
                                clearAllSinhalasubListeners();

                            } catch (downloadError) {
                                console.error('Download link error:', downloadError);
                                await socket.sendMessage(sender, {
                                     image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                                    caption: formatMessage(
                                        '❌ DOWNLOAD ERROR',
                                        `*Download link එක ලබාගැනීමේ දෝෂයක්.*\nError: ${downloadError.message}`,
                                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                    )
                                }, { quoted: downloadMek });
                            }
                        }
                    };

                   
                    sinhalasubDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', handleDownload);

                   
                    sinhalasubDownloadTimeout = setTimeout(() => {
                        if (sinhalasubDownloadListener) {
                            socket.ev.off('messages.upsert', sinhalasubDownloadListener);
                            sinhalasubDownloadListener = null;
                            console.log('🧹 Sinhalasub download listener timeout - cleaned up');
                        }
                        sinhalasubDownloadTimeout = null;
                    }, 120000);

                } catch (infoError) {
                    console.error('Movie info error:', infoError);
                    await socket.sendMessage(sender, {
                         image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
                        caption: formatMessage(
                            '❌ ERROR',
                            `*Movie details ලබාගැනීමේ දෝෂයක්:* ${infoError.message}`,
                            `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                        )
                    }, { quoted: replyMek });
                }
            }
        };

       
        sinhalasubSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', handleSelection);

       
        sinhalasubSelectionTimeout = setTimeout(() => {
            if (sinhalasubSelectionListener) {
                socket.ev.off('messages.upsert', sinhalasubSelectionListener);
                sinhalasubSelectionListener = null;
                console.log('🧹 Sinhalasub selection listener timeout - cleaned up');
            }
            sinhalasubSelectionTimeout = null;
        }, 120000);

    } catch (error) {
        console.error('Movie command error:', error);
       
        clearAllSinhalasubListeners();
        await socket.sendMessage(sender, {
             image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }
    break;
                 
    break;    case 'menu':
               case 'help':     {
    try {
        const pushName = msg.pushName || 'User';
        const date = new Date();
        const slstDate = new Date(date.toLocaleString("en-US", { timeZone: "Asia/Colombo" }));
        const formattedDate = `${slstDate.getFullYear()}/${slstDate.getMonth() + 1}/${slstDate.getDate()}`;
        const formattedTime = slstDate.toLocaleTimeString();
        
        const hour = slstDate.getHours();
      
        const greetings = hour < 12 ? `Good Morning✨` :
                          hour < 15 ? `Good Afternoon🚀` :
                          hour < 18 ? `Good Evening! 🌟` : `Good Night🌙`;
        const prefix = sessionConfig.PREFIX || config.PREFIX || '.';

        // Main Menu (Number reply removed)
        const mainMenuMsg = `*🌟 𝙃𝙚𝙮 ❟ ${pushName} ✨𝙃𝙤𝙬 𝙖𝙧𝙚 𝙮𝙤𝙪.*      
*╭─「 ᴄᴏᴍᴍᴀɴᴅꜱ ᴘᴀɴᴇʟ」*
*┃ \`🐸 ${greetings}\`*
*┃ \`🧩 𝚃𝚒𝚖𝚎\` : ${formattedTime}*
*┃ \`🦊 𝙳𝚊𝚝𝚎\` : ${formattedDate}*
*┃ \`🤡 𝙱𝚘𝚝 𝙽𝚊𝚖𝚎:\` 𝖲ʜᴀɢɢY-xᴍᴅ ⭐*
*┃ \`🐞 𝙿𝚕𝚊𝚝𝚏𝚘𝚛𝚖:\` Linux*
*╰────────●●►*    
╭─  ♡  ᴄᴏᴍᴍᴀɴᴅꜱ  ♡  ─╮

🎬  𝗠𝗼𝘃𝗶𝗲 & 𝗗𝗼𝘄𝗻𝗹𝗼𝗮𝗱
  • .cinesubz    — Movie dl
  • .dubzone     — Movie dl
  • .sinhalasub  — Movie dl
  • .anime       — Anime dl
  • .scartoon    — Cartoon dl
  • .song        — Music dl
  • .tiktok      — Tiktok dl
  • .ig          — Insta video dl
  • .fb          — Fb video dl

⚙️  𝗚𝗲𝗻𝗲𝗿𝗮𝗹
  • .alive       — status
  • .menu        — menu
  • .help        — menu
  • .set         — settings
  • .system      — info
  • .ping        — system info
  • .bots        — active session

  ⚙️  𝗢𝘁𝗵𝗲𝗿
• .ai           — Conversation 
• .schedule    — custom masej

╰─  © 𝚂ʜᴀɢY-xᴍᴅ  ─╯
> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
            caption: mainMenuMsg
        }, { quoted: msg });

       

    } catch (e) {
        console.error(e);
    }
}
break;   
case 'lakvision':
case 'teledrama': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර නම ලබාදෙන්න! උදා: .lakvision Aladin*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const searchQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/cartoons/lakvision';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let lakvisionSelectionListener = null;
    let lakvisionDownloadListener = null;
    let lakvisionMasterTimeout = null;

    const clearAllLakvisionListeners = () => {
        if (lakvisionSelectionListener) {
            socket.ev.off('messages.upsert', lakvisionSelectionListener);
            lakvisionSelectionListener = null;
        }
        if (lakvisionDownloadListener) {
            socket.ev.off('messages.upsert', lakvisionDownloadListener);
            lakvisionDownloadListener = null;
        }
        if (lakvisionMasterTimeout) {
            clearTimeout(lakvisionMasterTimeout);
            lakvisionMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching on Lakvision...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: searchQuery, api_key: API_KEY },
            timeout: 20000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු ප්‍රතිඵලයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const itemList = searchData.data.slice(0, 20);
        let listText = `📺 *𝗟𝗔𝗞𝗩𝗜𝗦𝗜𝗢𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${searchQuery}_*\n╭──────●➤\n*🔢 ʀᴇᴘ𝗹ʏ ʙ𝗲𝗹𝗼𝘄 ɴᴜᴍʙᴇ𝗥*\n╰──────────●➤\n╭──────●➤\n`;

        itemList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.type || 'Teledrama'} | ${item.quality || 'HD'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: itemList[0].image || sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        lakvisionMasterTimeout = setTimeout(() => {
            clearAllLakvisionListeners();
        }, 180000);

        const handleSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= itemList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${itemList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (lakvisionSelectionListener) {
                    socket.ev.off('messages.upsert', lakvisionSelectionListener);
                    lakvisionSelectionListener = null;
                }

                const chosenItem = itemList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching details & quality options...' }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenItem.link, api_key: API_KEY },
                        timeout: 20000
                    });

                    const details = infoRes.data?.data;
                    const downloads = details?.downloads || [];

                    if (!details || downloads.length === 0) {
                        throw new Error('බාගත කිරීමේ links හමු නොවීය.');
                    }

                    let infoText = `✨ *${details.title}* ✨\n\n`;
                    infoText += `🗣️ *Language:* ${details.language || 'Sinhala'}\n`;
                    infoText += `🎭 *Genres:* ${details.genres?.join(', ') || 'Teledrama'}\n`;
                    infoText += `📖 *Story:* ${details.story?.substring(0, 110) || 'N/A'}...\n\n`;
                    infoText += `📥 *SELECT QUALITY & FILE SIZE:*\n`;
                    infoText += `╭──────────────────●➤\n`;

                    downloads.forEach((dl, i) => {
                        const rawName = (dl.quality || dl.name || `Option ${i + 1}`).toLowerCase();
                        let sizeTag = dl.size ? `📦 *[ ${dl.size} ]*` : '📦 [Custom Size]';

                        if (!dl.size) {
                            if (rawName.includes('480p') || rawName.includes('sd')) {
                                sizeTag = '📉 *[ 300MB ]*';
                            } else if (rawName.includes('720p')) {
                                sizeTag = '💻 *[ 1GB ]*';
                            } else if (rawName.includes('1080p') || rawName.includes('fhd')) {
                                sizeTag = '🔥 *[ 2GB ]*';
                            } else {
                                if (i === 0) sizeTag = '📉 *[ 300MB ]*';
                                else if (i === 1) sizeTag = '💻 *[ 1GB ]*';
                                else if (i >= 2) sizeTag = '🔥 *[ 2GB ]*';
                            }
                        }

                        infoText += `*┃ ${i + 1} ❭❭ ${dl.name}* \n    ↳ ${sizeTag}\n`;
                    });
                    infoText += `╰──────────────────●➤\n\n👉 *අවශ්‍ය Quality එකේ අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: details.image || chosenItem.image },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleDownload = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message || dlMek.key.remoteJid !== sender) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const isDlReply = dlMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isDlReply) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= downloads.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${downloads.length} අතර නිවැරදි අංකයක් ලබාදෙන්න!` 
                                }, { quoted: dlMek });
                                return;
                            }

                            clearAllLakvisionListeners();
                            const selectedDl = downloads[dlIdx];
                            const cleanTitle = details.title.replace(/[\\/:*?"<>|]/g, '').trim();
                            const selectedQualityName = selectedDl.quality || selectedDl.name || 'HD';

                            await socket.sendMessage(sender, { react: { text: '⬇️', key: dlMek.key } });

                            if (selectedDl.type === 'dailymotion' || selectedDl.link.includes('.m3u8')) {
                                await socket.sendMessage(sender, {
                                    text: `✅ *LAKVISION LINK READY*\n\n` +
                                          `🎬 *Title:* ${details.title}\n` +
                                          `📊 *Option:* ${selectedDl.name}\n` +
                                          `🔗 *Watch / Download Link:*\n${selectedDl.link}\n\n` +
                                          `> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                                return;
                            }

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading:* ${selectedQualityName}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, වීඩියෝව ඩවුන්ලෝඩ් වෙමින් පවතී..._` 
                            }, { quoted: dlMek });

                            try {
                                await socket.sendMessage(sender, {
                                    document: { url: selectedDl.link },
                                    mimetype: 'video/mp4',
                                    fileName: `${cleanTitle} [${selectedQualityName}].mp4`,
                                    caption: `✅ *LAKVISION DOWNLOADED*\n\n🎬 *Title:* ${details.title}\n📊 *Quality:* ${selectedQualityName}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ ෆයිල් එක යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 *Direct Download Link:*\n${selectedDl.link}` 
                                }, { quoted: dlMek });
                            }
                        }
                    };

                    lakvisionDownloadListener = handleDownload;
                    socket.ev.on('messages.upsert', handleDownload);

                } catch (infoErr) {
                    clearAllLakvisionListeners();
                    await socket.sendMessage(sender, { text: `❌ Details Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        lakvisionSelectionListener = handleSelection;
        socket.ev.on('messages.upsert', handleSelection);

    } catch (err) {
        clearAllLakvisionListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
                                case 'sinama':
case 'sinamalka': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ නම ලබාදෙන්න! උදා: .sinama Marco*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const sinamaQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/movies/subz';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let sinamaSelectionListener = null;
    let sinamaDlSelectionListener = null;
    let sinamaMasterTimeout = null;

    const clearAllSinamaListeners = () => {
        if (sinamaSelectionListener) {
            socket.ev.off('messages.upsert', sinamaSelectionListener);
            sinamaSelectionListener = null;
        }
        if (sinamaDlSelectionListener) {
            socket.ev.off('messages.upsert', sinamaDlSelectionListener);
            sinamaDlSelectionListener = null;
        }
        if (sinamaMasterTimeout) {
            clearTimeout(sinamaMasterTimeout);
            sinamaMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching movies on Subz.lk...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: sinamaQuery, api_key: API_KEY },
            timeout: 20000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු චිත්‍රපටයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const movieList = searchData.data.slice(0, 20);
        let listText = `🎬 *𝗦𝗨𝗕𝗭.𝗟𝗞 𝗦𝗘𝗔𝗥𝗖𝗛 : _${sinamaQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙ𝗲𝗹𝗼𝘄 ɴᴜᴍʙ𝙚𝗥*\n╰──────────●➤\n╭──────●➤\n`;

        movieList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.quality || 'HD'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: movieList[0].image || sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        sinamaMasterTimeout = setTimeout(() => {
            clearAllSinamaListeners();
        }, 120000);

        const handleSinamaSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const contextInfo = replyMek.message.extendedTextMessage?.contextInfo;
            const isReply = contextInfo?.stanzaId === searchMsgID;

            if (isReply && sender === replyMek.key.remoteJid) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= movieList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${movieList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (sinamaSelectionListener) {
                    socket.ev.off('messages.upsert', sinamaSelectionListener);
                    sinamaSelectionListener = null;
                }

                const chosenMovie = movieList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching movie details & download options...' }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenMovie.link, api_key: API_KEY },
                        timeout: 20000
                    });

                    const movieData = infoRes.data?.data;
                    const downloads = movieData?.downloads || [];

                    if (!movieData || downloads.length === 0) {
                        throw new Error('බාගත කිරීමේ links හෝ විස්තර හමු නොවීය.');
                    }

                    let infoText = `✨ *${movieData.title}* ✨\n\n`;
                    infoText += `⭐ *IMDb:* ${movieData.imdb || 'N/A'}\n`;
                    infoText += `🗣️ *Language:* ${movieData.language || 'N/A'}\n`;
                    infoText += `📖 *Story:* ${movieData.story ? movieData.story.substring(0, 110) + '...' : 'N/A'}\n\n`;
                    infoText += `📥 *SELECT DOWNLOAD OPTION:*\n`;
                    infoText += `╭──────────────────●➤\n`;

                    downloads.forEach((dl, i) => {
                        const dlName = dl.name || `Option ${i + 1}`;
                        infoText += `*┃ ${i + 1} ❭❭ ${dlName}*\n`;
                    });
                    infoText += `╰──────────────────●➤\n\n👉 *අවශ්‍ය විකල්පයේ අංකය Reply කරන්න.*`;

                    let imgUrl = chosenMovie.image;
                    if (movieData.image && movieData.image !== 'https://subz.lk/wp-content/themes/subz/img/subzlk-logo.png') {
                        imgUrl = movieData.image;
                    }

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: imgUrl },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleSinamaDlSelection = async ({ messages: dlMessages }) => {
                        const dlMek = dlMessages?.[0];
                        if (!dlMek?.message) return;

                        const dlChoiceText = (dlMek.message.conversation || dlMek.message.extendedTextMessage?.text || '').trim();
                        const dlContextInfo = dlMek.message.extendedTextMessage?.contextInfo;
                        const isDlReply = dlContextInfo?.stanzaId === infoMsgID;

                        if (isDlReply && sender === dlMek.key.remoteJid) {
                            const dlIdx = parseInt(dlChoiceText) - 1;
                            if (isNaN(dlIdx) || dlIdx < 0 || dlIdx >= downloads.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${downloads.length} අතර නිවැරදි අංකයක් ලබාදෙන්න!` 
                                }, { quoted: dlMek });
                                return;
                            }

                            clearAllSinamaListeners();

                            const selectedDl = downloads[dlIdx];
                            const finalLink = selectedDl.direct_link || selectedDl.link;
                            const selectedName = selectedDl.name || 'Download Option';
                            const cleanTitle = movieData.title.replace(/[\\/:*?"<>|]/g, '').trim();

                            if (!finalLink) {
                                await socket.sendMessage(sender, { text: `❌ මෙම විකල්පය සඳහා ලින්ක් එකක් හමු නොවීය!` }, { quoted: dlMek });
                                return;
                            }

                            // මැග්නට් ලින්ක් (Magnet links) සඳහා ඩොකියුමෙන්ට් එකක් ලෙස යැවිය නොහැකි නිසා ටෙක්ස්ට් එකක් ලෙස ලබාදීම
                            if (finalLink.startsWith('magnet:?')) {
                                await socket.sendMessage(sender, {
                                    text: `🧲 *MAGNET LINK*\n\n` +
                                          `🎬 *Title:* ${movieData.title}\n` +
                                          `📦 *Option:* ${selectedName}\n\n` +
                                          `\`\`\`${finalLink}\`\`\`\n\n` +
                                          `> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });
                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                                return;
                            }

                            await socket.sendMessage(sender, { react: { text: '⬇️', key: dlMek.key } });

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading:* ${selectedName}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, ෆයිල් එක ඩවුන්ලෝඩ් වෙමින් පවතී..._` 
                            }, { quoted: dlMek });

                            try {
                                let mimetype = 'application/zip';
                                let ext = '.zip';
                                const lowerName = selectedName.toLowerCase();
                                const lowerLink = finalLink.toLowerCase();

                                if (lowerLink.includes('.torrent') || lowerName.includes('torrent file')) {
                                    mimetype = 'application/x-bittorrent';
                                    ext = '.torrent';
                                } else if (lowerLink.includes('.mp4') || lowerName.includes('mp4')) {
                                    mimetype = 'video/mp4';
                                    ext = '.mp4';
                                } else if (lowerLink.includes('.mkv') || lowerName.includes('mkv')) {
                                    mimetype = 'video/mkv';
                                    ext = '.mkv';
                                }

                                await socket.sendMessage(sender, {
                                    document: { url: finalLink },
                                    mimetype: mimetype,
                                    fileName: `${cleanTitle} [${selectedName.replace(/[^a-zA-Z0-9]/g, '_')}]${ext}`,
                                    caption: `✅ *SUBZ.LK FILE DOWNLOADED*\n\n🎬 *Title:* ${movieData.title}\n📦 *Option:* ${selectedName}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: dlMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: dlMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ ෆයිල් එක යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${finalLink}` 
                                }, { quoted: dlMek });
                            }
                        }
                    };

                    sinamaDlSelectionListener = handleSinamaDlSelection;
                    socket.ev.on('messages.upsert', handleSinamaDlSelection);

                } catch (infoErr) {
                    clearAllSinamaListeners();
                    await socket.sendMessage(sender, { text: `❌ Subz Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        sinamaSelectionListener = handleSinamaSelection;
        socket.ev.on('messages.upsert', handleSinamaSelection);

    } catch (err) {
        clearAllSinamaListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}

     // ==========================================

case 'schedule':
case 'remind': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    const input = args.join(' ');
    const parts = input.split('|');

    if (parts.length < 3) {
        let helpText = `🎀 *𝗦𝗖𝗛𝗘𝗗𝗨𝗟𝗘𝗥  𝗠𝗔𝗡𝗔𝗚𝗘𝗥*\n\n` +
            `📝 *𝖴𝗌𝖺𝗀𝖾 :* \`.schedule NUMBER | MESSAGE | TIME\`\n` +
            `✨ *𝖤𝗑𝖺𝗆𝗉𝗅𝖾 :* \`.schedule 94768069800 | Hello Bro | 30s\`\n` +
            `🫧 *𝖦𝗋𝗈𝗎𝗉 𝖤𝗑 :* \`.schedule 1203630...g.us | Meeting start! | 5m\`\n\n` +
            `🐞 *Time Units :* \`s\` (seconds), \`m\` (minutes), \`h\` (hours)\n` +
            `⚠️ *Note :* Use \`|\` (pipe) to separate parts.`;

        return await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE || config.ERROR },
            caption: formatMessage(
                `⏰ 𝗦𝗖𝗛𝗘𝗗𝗨𝗟𝗘  𝗖𝗢𝗠𝗠𝗔𝗡𝗗`,
                helpText,
                `${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
            )
        }, { quoted: msg });
    }

    let target = parts[0].trim();
    let reminderMsg = parts[1].trim(); // මැසේජ් එක දැන් දෙවනියට තියෙන්නේ
    let timeArg = parts[2].trim();     // වෙලාව දැන් තුන්වනියට තියෙන්නේ

    // Number එකක් නම් JID එකකට හරවා ගැනීම
    if (!target.includes('@s.whatsapp.net') && !target.includes('@g.us')) {
        target = target.replace(/[^0-9]/g, '') + '@s.whatsapp.net';
    }

    // Time එක convert කරගැනීම (s, m, h)
    const unit = timeArg.slice(-1).toLowerCase();
    const value = parseInt(timeArg.slice(0, -1));

    if (isNaN(value) || value <= 0 || !['s', 'm', 'h'].includes(unit)) {
        return await socket.sendMessage(sender, {
            text: `❌ *Invalid time format!*\nUse like: \`30s\`, \`5m\`, or \`1h\` at the end.`
        }, { quoted: msg });
    }

    let delayMs = value * 1000;
    if (unit === 'm') delayMs = value * 60 * 1000;
    if (unit === 'h') delayMs = value * 60 * 60 * 1000;

    if (delayMs > 24 * 60 * 60 * 1000) {
        return await socket.sendMessage(sender, {
            text: `❌ *Time limit exceeded!* Maximum schedule time is 24 hours.`
        }, { quoted: msg });
    }

    await socket.sendMessage(sender, {
        text: `⏳ *Scheduled successfully!*\nTarget: \`${target}\`\nTime: *${timeArg}*`
    }, { quoted: msg });

    // නියමිත වෙලාව ආවම වෙනත් අමතර වැකි නැතුව අදාළ මැසේජ් එක විතරක් යැවීම
    setTimeout(async () => {
        try {
            await socket.sendMessage(target, {
                text: reminderMsg
            });
        } catch (err) {
            console.error("Schedule Send Error:", err);
        }
    }, delayMs);
}
break;


                    case 'ai':
case 'codex': {
    const query = args.join(' ');
    if (!query && !msg.hasMedia) {
        return await socket.sendMessage(sender, {
            text: `❌ *What do you want to ask Codex AI?*\n✨ *Example:* \`\`.ai Quantum computing kiyanne mokakda?\`\``
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "🤖", key: msg.key } });

        let imageUrl = null;
        let videoUrl = null;

        // Image / Media support (කොටස් වලට photo එකක් හෝ caption එකක් එක්ක photo එකක් එව්වොත් handle කරන්න)
        const quotedMessage = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
        const isQuotedImage = quotedMessage?.imageMessage;
        const isDirectImage = msg.message?.imageMessage;

        if (isDirectImage || isQuotedImage) {
            // Media download කිරීම සඳහා Baileys වල downloadMediaMessage පාවිච්චි කරයි
            const stream = await downloadMediaMessage(
                isDirectImage ? msg : { message: quotedMessage },
                'buffer',
                {},
                { logger: console }
            );
            
            const mimeType = isDirectImage ? msg.message.imageMessage.mimetype : quotedMessage.imageMessage.mimetype;
            imageUrl = `data:${mimeType};base64,${stream.toString('base64')}`;
        }

        const CODEX_API_KEY = "cx_live_555l2y4l5a5t0y5z1x5a1i4j221o5h3j";
        const CODEX_URL = "https://code-x-ai.lovable.app/api/public/v1/chat";

        const apiResponse = await fetch(CODEX_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${CODEX_API_KEY}`
            },
            body: JSON.stringify({
                message: query || "Meke thiyenne mokakda?",
                session: sender,          // chat id එකම session එක විදිහට දීලා long-term memory active කිරීම
                image_url: imageUrl       // Vision / Photo support එක
            })
        });

        const resData = await apiResponse.json();
        const aiReply = resData.reply || resData.error || "Sorry, I couldn't process that.";

        await socket.sendMessage(sender, {
            text: `${aiReply}\n\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });

    } catch (err) {
        console.error("Codex AI Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Codex AI service is currently busy.*` }, { quoted: msg });
    }
}
break;

   // ==========================================
// 1. SYSTEM / PING COMMAND
// ==========================================
case 'system':
case 'ping':
case 'status': {
    try {
        await socket.sendMessage(sender, { react: { text: "⚡", key: msg.key } });

        const os = await import('os');
        const startTime = process.hrtime();
        const diff = process.hrtime(startTime);
        const latency = (diff[0] * 1e3 + diff[1] * 1e-6).toFixed(4);

        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = totalMem - freeMem;

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        const uptimeSeconds = process.uptime();
        const days = Math.floor(uptimeSeconds / (3600 * 24));
        const hours = Math.floor((uptimeSeconds % (3600 * 24)) / 3600);
        const minutes = Math.floor((uptimeSeconds % 3600) / 60);
        const seconds = Math.floor(uptimeSeconds % 60);
        const uptimeFormatted = `${days}d ${hours}h ${minutes}m ${seconds}s`;

        let systemText = `🖥️ *𝗦𝗛𝗔𝗚𝗚𝗬  𝗫𝗠𝗗  -  𝗦𝗬𝗦𝗧𝗘𝗠  𝗦𝗧𝗔𝗧𝗨𝗦* 📊\n\n` +
            `⚡ *𝖲ᵵᵃᵗᵘˢ 𝖲ᵖᵉᵉᵈ :* \`${latency} ms\`\n` +
            `⏳ *𝖴ᵖᵗⁱᵐᵉ :* \`${uptimeFormatted}\`\n` +
            `🧠 *𝖱𝖠𝖬 𝖴𝗌𝖺𝗀𝖾 :* \`${formatBytes(usedMem)} / ${formatBytes(totalMem)}\`\n` +
            `🌐 *𝖯𝖑ᵃᵗᶠᵒʳᵐ :* \`${os.platform()} (${os.arch})\`\n\n` +
            `> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`;

        await socket.sendMessage(sender, { text: systemText }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } });
    } catch (err) {
        console.error("System Cmd Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Failed to fetch system status.*` }, { quoted: msg });
    }
}
break;

// ==========================================
// 2. BOTS / SESSIONS COMMAND
// ==========================================
case 'sessions':
case 'connectedbots':
case 'bots': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "🔍", key: msg.key } });

        const mongoose = (await import('mongoose')).default;
        const db = mongoose.connection.db;

        if (!db) {
            return await socket.sendMessage(sender, { text: `❌ *MongoDB connection is not active!*` }, { quoted: msg });
        }

        const collections = await db.listCollections().toArray();
        let sessionData = [];
        let foundCollectionName = '';

        const targetColl = collections.find(c => 
            c.name.toLowerCase().includes('session') || 
            c.name.toLowerCase().includes('auth') || 
            c.name.toLowerCase().includes('bot') ||
            c.name.toLowerCase().includes('baileys')
        );

        if (targetColl) {
            foundCollectionName = targetColl.name;
            const collection = db.collection(foundCollectionName);
            sessionData = await collection.find({}).limit(15).toArray();
        }

        let sessionText = `🤖 *𝗦𝗛𝗔𝗚𝗚𝗬  𝗫𝗠𝗗  -  𝗖𝗢𝗡𝗡𝗘𝗖𝗧𝗘𝗗  𝗕𝗢𝗧𝗦 / 𝗦𝗘𝗦𝗦𝗜𝗢𝗡𝗦* 🌐\n\n` +
            `📂 *Collection :* \`${foundCollectionName || 'None'}\`\n` +
            `📊 *Active Count :* \`${sessionData.length} Records\`\n\n`;

        if (sessionData.length > 0) {
            sessionData.forEach((ses, index) => {
                const num = index + 1;
                const idStr = JSON.stringify(ses._id || ses.id || 'Unknown');
                sessionText += `*${num}.* \`${idStr.replace(/["']/g, '')}\`\n`;
            });
        } else {
            sessionText += `_No active session keys found in MongoDB collections._\n`;
        }

        sessionText += `\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`;

        await socket.sendMessage(sender, { text: sessionText }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } });

    } catch (err) {
        console.error("Sessions Cmd Error:", err);
        await socket.sendMessage(sender, { text: `❌ *Failed to fetch connected bots: ${err.message}*` }, { quoted: msg });
    }
}
break;
case 'cartoon':
case 'sinhalacartoon': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර කාටූනයේ නම ලබාදෙන්න! උදා: .cartoon Ben 10*',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const cartoonQuery = args.join(' ');
    const API_BASE = 'https://api.chamindu.site/api/v1/cartoons/sinhalacartoons';
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';

    let cartoonSelectionListener = null;
    let cartoonEpisodeListener = null;
    let cartoonMasterTimeout = null;

    const clearAllCartoonListeners = () => {
        if (cartoonSelectionListener) {
            socket.ev.off('messages.upsert', cartoonSelectionListener);
            cartoonSelectionListener = null;
        }
        if (cartoonEpisodeListener) {
            socket.ev.off('messages.upsert', cartoonEpisodeListener);
            cartoonEpisodeListener = null;
        }
        if (cartoonMasterTimeout) {
            clearTimeout(cartoonMasterTimeout);
            cartoonMasterTimeout = null;
        }
    };

    try {
        await socket.sendMessage(sender, { text: '🔍 Searching cartoons on SinhalaCartoons...' }, { quoted: msg });

        const searchRes = await axios.get(`${API_BASE}/search`, {
            params: { q: cartoonQuery, api_key: API_KEY },
            timeout: 20000
        });

        const searchData = searchRes.data;
        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    '*කිසිදු කාටූනයක් හමු නොවීය!*',
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const cartoonList = searchData.data.slice(0, 20);
        let listText = `🧸 *𝗦𝗜𝗡𝗛𝗔𝗟𝗔 𝗖𝗔𝗥𝗧𝗢𝗢𝗡 𝗦𝗘𝗔𝗥𝗖𝗛 : _${cartoonQuery}_*\n╭──────●➤\n*🔢 ʀᴇ𝗽𝗹ʏ ʙᴇʟ𝗼ᴡ ɴᴜᴍʙᴇʀ*\n╰──────────●➤\n╭──────●➤\n`;

        cartoonList.forEach((item, index) => {
            listText += `*🧩 ${index + 1} ┃❭❭ ${item.title}*\n    ↳ (${item.quality || 'HD'} | ⭐ ${item.rating || 'N/A'})\n`;
        });
        listText += `╰──────────●➤\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        const searchMsg = await socket.sendMessage(sender, {
            image: { url: cartoonList[0].image || sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const searchMsgID = searchMsg.key.id;

        cartoonMasterTimeout = setTimeout(() => {
            clearAllCartoonListeners();
        }, 120000);

        const handleCartoonSelection = async ({ messages }) => {
            const replyMek = messages?.[0];
            if (!replyMek?.message || replyMek.key.remoteJid !== sender) return;

            const text = (replyMek.message.conversation || replyMek.message.extendedTextMessage?.text || '').trim();
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === searchMsgID;

            if (isReply) {
                const choice = parseInt(text) - 1;
                if (isNaN(choice) || choice < 0 || choice >= cartoonList.length) {
                    await socket.sendMessage(sender, {
                        text: `❌ කරුණාකර 1 - ${cartoonList.length} අතර අංකයක් ලබාදෙන්න!`
                    }, { quoted: replyMek });
                    return;
                }

                if (cartoonSelectionListener) {
                    socket.ev.off('messages.upsert', cartoonSelectionListener);
                    cartoonSelectionListener = null;
                }

                const chosenCartoon = cartoonList[choice];
                await socket.sendMessage(sender, { text: '⏳ Fetching cartoon details & episodes...' }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(`${API_BASE}/infodl`, {
                        params: { q: chosenCartoon.link, api_key: API_KEY },
                        timeout: 20000
                    });

                    const cartoonData = infoRes.data?.data;
                    const allDownloads = cartoonData?.downloads || [];

                    if (!cartoonData || allDownloads.length === 0) {
                        throw new Error('බාගත කිරීමේ links හෝ episodes හමු නොවීය.');
                    }

                    const directDownloads = allDownloads.filter(d => d.link?.endsWith('.mp4') || !d.name?.includes('Telegram'));
                    const finalDownloads = directDownloads.length > 0 ? directDownloads : allDownloads;

                    let infoText = `🍀 *${cartoonData.title}*\n\n`;
                    infoText += `⭐ *IMDb:* ${cartoonData.imdb || 'N/A'}\n`;
                    infoText += `🗣️ *Language:* ${cartoonData.language || 'Sinhala'}\n`;
                    infoText += `🎭 *Genres:* ${cartoonData.genres?.join(', ') || 'Cartoon'}\n\n`;
                    infoText += `*Available Episodes / Links:*\n`;

                    finalDownloads.forEach((dl, i) => {
                        infoText += `*${i + 1}.* ${dl.name}\n`;
                    });
                    infoText += `\n👉 *බාගත කිරීමට අදාළ Episode අංකය Reply කරන්න.*`;

                    const infoMsg = await socket.sendMessage(sender, {
                        image: { url: cartoonData.image || chosenCartoon.image },
                        caption: infoText
                    }, { quoted: replyMek });

                    const infoMsgID = infoMsg.key.id;

                    const handleEpisodeSelection = async ({ messages: epMessages }) => {
                        const epMek = epMessages?.[0];
                        if (!epMek?.message || epMek.key.remoteJid !== sender) return;

                        const epChoiceText = (epMek.message.conversation || epMek.message.extendedTextMessage?.text || '').trim();
                        const isEpReply = epMek.message.extendedTextMessage?.contextInfo?.stanzaId === infoMsgID;

                        if (isEpReply) {
                            const epIdx = parseInt(epChoiceText) - 1;
                            if (isNaN(epIdx) || epIdx < 0 || epIdx >= finalDownloads.length) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ කරුණාකර 1 - ${finalDownloads.length} අතර Episode අංකයක් ලබාදෙන්න!` 
                                }, { quoted: epMek });
                                return;
                            }

                            clearAllCartoonListeners();
                            const selectedEpisode = finalDownloads[epIdx];

                            await socket.sendMessage(sender, { react: { text: '📥', key: epMek.key } });

                            await socket.sendMessage(sender, { 
                                text: `⏳ *Downloading Episode:* ${selectedEpisode.name}\n_කරුණාකර ටික වේලාවක් රැඳී සිටින්න, වීඩියෝව ඩවුන්ලෝඩ් වෙමින් පවතී..._` 
                            }, { quoted: epMek });

                            try {
                                // Direct Link එක වෙනුවට Document MP4 එකක් ලෙස යැවීම
                                await socket.sendMessage(sender, {
                                    document: { url: selectedEpisode.link },
                                    mimetype: 'video/mp4',
                                    fileName: `${cartoonData.title} - ${selectedEpisode.name}.mp4`,
                                    caption: `✅ *CARTOON DOWNLOADED*\n\n🎬 *Series:* ${cartoonData.title}\n📌 *Episode:* ${selectedEpisode.name}\n> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                                }, { quoted: epMek });

                                await socket.sendMessage(sender, { react: { text: '✅', key: epMek.key } });
                            } catch (uploadErr) {
                                await socket.sendMessage(sender, { 
                                    text: `❌ වීඩියෝව යැවීමේදී දෝෂයක් ඇති විය: ${uploadErr.message}\n\n🔗 Direct Link එක: ${selectedEpisode.link}` 
                                }, { quoted: epMek });
                            }
                        }
                    };

                    cartoonEpisodeListener = handleEpisodeSelection;
                    socket.ev.on('messages.upsert', handleEpisodeSelection);

                } catch (infoErr) {
                    clearAllCartoonListeners();
                    await socket.sendMessage(sender, { text: `❌ Cartoon Info Error: ${infoErr.message}` }, { quoted: replyMek });
                }
            }
        };

        cartoonSelectionListener = handleCartoonSelection;
        socket.ev.on('messages.upsert', handleCartoonSelection);

    } catch (err) {
        clearAllCartoonListeners();
        await socket.sendMessage(sender, {
            text: `❌ Error: ${err.message}`
        }, { quoted: msg });
    }
    break;
}
case 'news':
    case 'siyatha': {
        try {
            const apiUrl = 'https://api-siteh-22e22e4cb068.herokuapp.com/news/siyatha?api_key=lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e';
            const response = await axios.get(apiUrl);
            const resData = response.data;

            if (resData.status && resData.result) {
                let newsItem = resData.result;
                let caption = `📰 *${newsItem.title}*\n\n` +
                              `📅 *Date:* ${newsItem.date}\n\n` +
                              `${newsItem.desc}\n\n` +
                              `🔗 *Link:* ${newsItem.link}`;

                await sock.sendMessage(from, { 
                    image: { url: newsItem.image }, 
                    caption: caption 
                }, { quoted: mek });
            } else {
                await sock.sendMessage(from, { text: '❌ පුවත් ලබාගැනීමේදී දෝෂයක් ඇති විය.' }, { quoted: mek });
            }
        } catch (e) {
            console.error(e);
            await sock.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: mek });
        }
        break;
    }

    case 'fitgirl':
    case 'fg': {
        try {
            if (!q) return await sock.sendMessage(from, { text: '❌ කරුණාකර සෙවිය යුතු ක්‍රීඩාවේ නමක් සඳහන් කරන්න!\nඋදා: `.fitgirl far cry`' }, { quoted: mek });

            const searchUrl = `https://api-siteh-22e22e4cb068.herokuapp.com/fitgirl/search?game=${encodeURIComponent(q)}`;
            const response = await axios.get(searchUrl);
            const resData = response.data;

            if (resData.status && resData.results && resData.results.length > 0) {
                let txt = "🎮 *FitGirl Repacks Search Results* 🎮\n\n";
                resData.results.forEach((game, index) => {
                    txt += "*" + (index + 1) + ".* " + game.title + "\n🔗 " + game.link + "\n\n";
                });
                txt += "*සම්පූර්ණ විස්තර බැලීමට .fginfo [game name] භාවිතා කරන්න.*";
                
                await sock.sendMessage(from, { text: txt }, { quoted: mek });
            }
        } catch (e) {
            console.error(e);
            await sock.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: mek });
        }
        break;
    }

    case 'fginfo':
    case 'fitgirlinfo': {
        try {
            if (!q) return await sock.sendMessage(from, { text: '❌ කරුණාකර game එකේ නම නිවැරදිව ලබා දෙන්න!\nඋදා: `.fginfo far cry 5`' }, { quoted: mek });

            const infoUrl = `https://api-siteh-22e22e4cb068.herokuapp.com/fitgirl/complete?game=${encodeURIComponent(q)}`;
            const response = await axios.get(infoUrl);
            const resData = response.data;

            if (resData.status && resData.data && resData.data.game) {
                let g = resData.data.game;
                let caption = `🎮 *${g.title}*\n\n` +
                              `📌 *Version:* ${g.version}\n` +
                              `🏢 *Companies:* ${g.companies}\n` +
                              `🌐 *Languages:* ${g.languages}\n` +
                              `📦 *Original Size:* ${g.original_size}\n` +
                              `💾 *Repack Size:* ${g.repack_size}\n` +
                              `🏷️ *Categories:* ${g.categories.join(', ')}\n` +
                              `📅 *Published:* ${g.published_date}`;

                await sock.sendMessage(from, { 
                    image: { url: g.poster }, 
                    caption: caption 
                }, { quoted: mek });
            } else {
                await sock.sendMessage(from, { text: '❌ අදාළ ක්‍රීඩාවේ තොරතුරු ලබා ගැනීමට නොහැකි විය.' }, { quoted: mek });
            }
        } catch (e) {
            console.error(e);
            await sock.sendMessage(from, { text: '❌ දෝෂයක් සිදු විය: ' + e.message }, { quoted: mek });
        }
        break;
    } 
// ==========================================
// SYSTEM CONFIGURATION & MONGODB SETTING COMMAND (.set)
// ==========================================
case 'set':
case 'setting': {
    if (!isOwner) {
        return await socket.sendMessage(sender, {
            text: "❌ *Only the bot owner can use this command.*"
        }, { quoted: msg });
    }

    if (!args.length) {
        let helpText = `🎀 *𝗦𝗬𝗦𝗧𝗘𝗠  𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡  𝗣𝗔𝗡𝗘𝗟*\n\n` +
            `📝 *𝖴𝗌𝖺𝗀𝖾 :* \`.set KEY:VALUE\`\n` +
            `✨ *𝖤𝗑𝖺𝗆𝗉𝗅𝖾 :* \`.set ALWAYS_ONLINE:true\`\n` +
            `🫧 *𝖬𝗎𝗅𝗍𝗂 :* \`.set ALWAYS_ONLINE:true,AUTO_RECORDING:true\`\n\n` +
            `🐞 *𝖠𝗏𝖺𝗂𝗅𝖺𝖻𝗅ե  𝖲𝗒𝗌𝗍𝖾𝗆  𝖪𝖾𝗒𝗌 :*\n` +
            `🐞 \`ALWAYS_ONLINE\` (true/false)\n` +
            `🐞 \`ALWAYS_MSG_SEEN\` (true/false)\n` +
            `🐞 \`AUTO_RECORDING\` (true/false)\n` +
            `🐞 \`AUTO_TYPING\` (true/false)\n` +
            `🐞 \`STATUS_VIEW\` (true/false)\n` +
            `🐞 \`AUTO_LIKE\` (true/false)\n` +
            `🐞 \`PREFIX\`\n` +
            `🐞 \`MODE\` (public/private)\n`;

        return await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE || config.ERROR },
            caption: formatMessage(
                `𝗖𝗢𝗡𝗙𝗜𝗚  𝗠𝗔𝗡𝗔𝗚𝗘𝗥  ⚙️`,
                helpText,
                `${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
            )
        }, { quoted: msg });
    }

    const input = args.join(' ');
    const updates = {};
    const validKeys = [
        'PREFIX', 'AUTO_RECORDING', 'AUTO_TYPING', 'MODE', 'JID',
        'ALWAYS_ONLINE', 'ALWAYS_MSG_SEEN', 'STATUS_VIEW', 'AUTO_LIKE'
    ];

    const pairs = input.split(',');
    let hasInvalidKey = false;
    let invalidKeyName = '';

    pairs.forEach(pair => {
        let [key, ...valueParts] = pair.split(':');
        if (!key || valueParts.length === 0) return;

        key = key.trim().toUpperCase();
        let value = valueParts.join(':').trim();

        if (validKeys.includes(key)) {
            if (value.toLowerCase() === 'true') {
                updates[key] = 'true';
            } else if (value.toLowerCase() === 'false') {
                updates[key] = 'false';
            } else {
                updates[key] = value;
            }
        } else {
            hasInvalidKey = true;
            invalidKeyName = key;
        }
    });

    if (hasInvalidKey) {
        return await socket.sendMessage(sender, {
            text: `Invalid system key: \`${invalidKeyName}\`\n\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
        }, { quoted: msg });
    }

    if (Object.keys(updates).length === 0) {
        return await socket.sendMessage(sender, { text: "🎀 *𝗙𝗢𝗥𝗠𝗔𝗧  𝗘𝗥𝗥𝗢𝗥:* Please use `Key:Value` structure." });
    }

    try {
        await socket.sendMessage(sender, { react: { text: "⚙️", key: msg.key } });

        // 1. Session සහ Database එක රියල්-ටයිම් අප්ඩේට් කිරීම
        sessionConfig = { ...sessionConfig, ...updates };

        // MongoDB වෙත ඩේටා නිවැරදිව සේව් වීම සඳහා updateUserConfig හෝ Mongoose Model එක හරහා ස්ථිරවම Save කරයි
        if (typeof updateUserConfig === 'function') {
            await updateUserConfig(sanitizedNumber, sessionConfig);
        } else {
            // ද බෝට්ගේ වෙනත් කෝඩ් එකක Model එක හරහා Save වන ආකාරය (მაგ: BotModel.findOneAndUpdate)
            const BotModel = require('./database/model'); // උඹේ ප්‍රොජෙක්ට් එකේ හැටියට මොඩල් පේජ් එක මෙතැනට සෙට් කරගන්න පුළුවන්
            await BotModel.findOneAndUpdate(
                { id: sanitizedNumber },
                { $set: sessionConfig },
                { upsert: true, new: true }
            );
        }

        // Active Sockets වලට අලුත් කොන්ෆිග් එක රියල්-ටයිම් ලෝඩ් කිරීම
        activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

        let updateSummary = Object.entries(updates).map(([k, v]) => {
            let displayVal = Array.isArray(v) ? v.join(' ') : v;
            return `🎀 *${k}* ──❯ \`${displayVal}\``;
        }).join('\n');

        const successMsg = `🎀 *𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡  𝗨𝗣𝗗𝗔𝗧𝗘𝗗*\n\n` +
            `${updateSummary}\n\n` +
            `🫧 _System cloud & MongoDB changes applied successfully._`;

        await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE || config.ERROR },
            caption: formatMessage(
                `✅ 𝗨𝗣𝗗𝗔𝗧𝗘  𝗦𝗨𝗖𝗖𝗘𝗦𝗦  ✅`,
                successMsg,
                `${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
            )
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });

    } catch (error) {
        console.error("Update Error:", error);
        await socket.sendMessage(sender, { text: "🎀 " + error.message });
    }
}
break;
        }
        } catch (error) {
            console.error('Command handler error:', error);
            await socket.sendMessage(sender, {
                text: `❌ ERROR\nAn error occurred: ${error.message}`,
            });
        }
    });
}

async function setupMessageHandlers(socket) {
    const messageHandler = async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const senderNumber = msg.key.participant ? msg.key.participant.split('@')[0] : msg.key.remoteJid.split('@')[0];
        const botNumber = jidNormalizedUser(socket.user.id).split('@')[0];
        const isReact = msg.message.reactionMessage;

        const sanitizedNumber = botNumber.replace(/[^0-9]/g, '');
        const sessionConfig = activeSockets.get(sanitizedNumber)?.config || config;

        if (sessionConfig.AUTO_TYPING === 'true') {
            try {
                await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
            } catch (error) {
                
            }
        }

        if (sessionConfig.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
            } catch (error) {
               
            }
        }

        if (!isReact && senderNumber !== botNumber) {
            if (sessionConfig.AUTO_REACT === 'true') {
                const reactions = [
                    '❤', '💕', '😻', '🧡', '💛', '💚', '💙', '💜', '🖤', '❣', '💞', '💓', '💗',
                    '💖', '💘', '💝', '💟', '♥', '💌', '🙂', '🤗', '😌', '😉', '🤗', '😊',
                    '🎊', '🎉', '🎁', '🎈', '👋'
                ];
                const randomReaction = reactions[Math.floor(Math.random() * reactions.length)];

                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 1000));

                try {
                    await socket.sendMessage(msg.key.remoteJid, { react: { text: randomReaction, key: msg.key } });
                } catch (error) {
                    
                }
            }
        }
    };

    socket.ev.on('messages.upsert', messageHandler);
    return () => {
        socket.ev.off('messages.upsert', messageHandler);
       
    };
}

async function saveSession(number, creds) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { creds, updatedAt: new Date() },
            { upsert: true }
        );
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
        }
        if (!numbers.includes(sanitizedNumber)) {
            numbers.push(sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {
      
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const session = await Session.findOne({ number: sanitizedNumber });
        if (!session || !session.creds || !session.creds.me || !session.creds.me.id) {
            await deleteSession(sanitizedNumber);
            return null;
        }
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(session.creds, null, 2));
        return session.creds;
    } catch (error) {
        return null;
    }
}

async function deleteSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.deleteOne({ number: sanitizedNumber });
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            fs.removeSync(sessionPath);
        }
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            numbers = numbers.filter(n => n !== sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {
        
    }
}

async function loadUserConfig(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const configDoc = await Session.findOne({ number: sanitizedNumber }, 'config');
        return { ...config, ...configDoc?.config };
    } catch (error) {
        console.error(`Failed to load config for ${number}:`, error);
        return { ...config };
    }
}

async function updateUserConfig(number, newConfig) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { config: newConfig, updatedAt: new Date() },
            { upsert: true }
        );
        console.log(`Updated config for ${sanitizedNumber}`);
    } catch (error) {
        console.error(`Failed to update config for ${sanitizedNumber}:`, error);
        throw error;
    }
} 
function setupAutoRestart(socket, number) {
    const maxReconnectAttempts = 10;
    let reconnectAttempts = 0;

    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'close' && lastDisconnect?.error?.output?.statusCode !== 401) {
            if (reconnectAttempts >= maxReconnectAttempts) {
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                return;
            }
            console.log(`Connection lost for ${number}, attempt ${reconnectAttempts + 1}/${maxReconnectAttempts}`);
            try {
                await delay(5000 * (reconnectAttempts + 1));
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                await EmpirePair(number, mockRes);
                reconnectAttempts = 0;
            } catch (error) {
                console.error(`Reconnect failed for ${number}:`, error);
                reconnectAttempts++;
            }
        } else if (connection === 'open') {
            reconnectAttempts = 0;
            console.log(`Connection established for ${number}`);
        }
    });
}
async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    await restoreSession(sanitizedNumber);
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

    try {
        const { version } = await fetchLatestBaileysVersion();
        const socket = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            version,
            browser: Browsers.macOS('Safari'),
        });

        socketCreationTime.set(sanitizedNumber, Date.now());
        setupCommandHandlers(socket, sanitizedNumber);
        setupAutoRestart(socket, sanitizedNumber);
        if (!socket.authState.creds.registered) {
            let retries = config.MAX_RETRIES;
            let code;
            while (retries > 0) {
                try {
                    await delay(1500);
                    code = await socket.requestPairingCode(sanitizedNumber);
                    break;
                } catch (error) {
                    retries--;
                    if (retries === 0) throw error;
                    await delay(2000 * (config.MAX_RETRIES - retries));
                }
            }
            if (!res.headersSent) res.send({ code });
        }
        socket.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                const credsPath = path.join(sessionPath, 'creds.json');
                if (!fs.existsSync(credsPath)) return;
                const creds = JSON.parse(await fs.readFile(credsPath, 'utf8'));
                await saveSession(sanitizedNumber, creds);
            } catch (error) {
            }
        });
        socket.ev.on('connection.update', async (update) => {
            const { connection } = update;

            if (connection === 'open') {
                try {
                    await delay(3000);
                    await socket.sendPresenceUpdate('unavailable');
                    try {
                        const lidStore = socket.signalRepository.lidMapping;
                        const userJid = jidNormalizedUser(socket.user.id);

                        if (isPnUser(userJid)) {
                            const lid = await lidStore.getLIDForPN(userJid);
                            console.log(`✅ ${sanitizedNumber} → PN: ${userJid} → LID: ${lid}`);
                        }
                    } catch (lidError) {
                        console.log(`⚠️ LID mapping not available yet for ${sanitizedNumber}:`, lidError.message);
                    }

                    setInterval(() => {
                        socket.sendPresenceUpdate('unavailable').catch(() => {});
                    }, 30000);

                    const userJid = jidNormalizedUser(socket.user.id);
                    let sessionConfig = await loadUserConfig(sanitizedNumber);
                    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

                    // Welcome Message
                    await socket.sendMessage(userJid, {
                        image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage(
                            '✨ *Bot Activated!*',
                            `📱 *Number:* ${sanitizedNumber}
🕒 *Time:* ${getSriLankaTimestamp()}
🟢 *Status:* Online`,
                            'simple & clean'
                        )
                    });

                } catch (error) {
                    console.error(`Error in connection.open for ${sanitizedNumber}:`, error);
                    exec(`pm2 restart ${process.env.PM2_NAME || '{LAKIYA-{M𝙳-{F𝚁𝙴𝙴-{B𝙾𝚃-session'}`);
                }
            }
        });

    } catch (error) {
        console.error('Pairing/reconnect error:', error);
        socketCreationTime.delete(sanitizedNumber);
        if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
    }
}

router.get('/', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).send({ error: 'Number parameter is required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    if (activeSockets.has(sanitizedNumber)) {
        try {
            const oldSocket = activeSockets.get(sanitizedNumber);
            if (oldSocket && oldSocket.socket) {
                try {
                    await oldSocket.socket.logout();
                    oldSocket.socket.end();
                    oldSocket.socket.ws?.close();
                } catch (e) {
                    console.log('Socket close error:', e.message);
                }
            }
            activeSockets.delete(sanitizedNumber);
            socketCreationTime.delete(sanitizedNumber);
            await Session.deleteOne({ number: sanitizedNumber });
            const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
            if (fs.existsSync(sessionPath)) {
                fs.removeSync(sessionPath);
            }
            if (fs.existsSync(NUMBER_LIST_PATH)) {
                let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
                numbers = numbers.filter(n => n !== sanitizedNumber);
                fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
            }
            console.log(`✅ Old session removed for: ${sanitizedNumber} - Creating new pairing`);
        } catch (error) {
            console.error('Error removing old session:', error);
        }
    }

    await EmpirePair(number, res);
});

process.on('exit', () => {
    activeSockets.forEach((socket, number) => {
        socket.ws.close();
        activeSockets.delete(number);
        socketCreationTime.delete(number);
    });
    fs.emptyDirSync(SESSION_BASE_PATH);
});

process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err);
    exec(`pm2 restart ${process.env.PM2_NAME || '{test-{md-{mini-{bot-session'}`);
});

export default router;
