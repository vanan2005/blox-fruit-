--[[
  BLOX FRUITS DATA COLLECTOR v3.0 / MULTI-SEA AREA ATLAS
  - Starts ONLY after user presses START in this standalone menu.
  - Read-only: no FireServer, InvokeServer, teleport, HTTP or hooks.
  - One Sea per game session. Multi-Sea coverage accumulates on disk.
  - Workspace streaming means no script can guarantee every unloaded area.
]]
local function runSnapshot(ctx)
--[[
    BLOX FRUITS GAME DATA COLLECTOR 3.0
    Read-only, standalone Luau data explorer / snapshot exporter.
    NOT an andepzai update. No movement, farming, remote firing or HTTP calls.

    Executor with writefile/makefolder recommended for JSON export.
    Only data replicated to YOUR client is observable. Run once per Sea,
    and repeat after moving to different areas if Workspace streaming is on.
]]

-- =================== CONFIG ===================
local OPTIONS = {
    FolderPrefix = "BloxFruits_Data",
    MaxInstanceRecords = 150000,      -- hard stop: mobile-friendly safety limit
    ChunkSize = 220,                  -- JSON records per file
    YieldEvery = 120,                 -- avoid long frozen frames
    ObserveSeconds = 35,             -- NPC + movement observation after scan (0 = disable)
    TrackMovementDiagnostics = true, -- read-only; no anti-cheat bypass or hooks
    MovementSampleInterval = 0.20,  -- lightweight position sampling in seconds
    LargeMovementStepStuds = 120,  -- log suspicious jumps, not regular 350-stud/s tween
    ReturnToleranceStuds = 25,      -- possible snapback near earlier position
    MovementMaxEvents = 350,
    WatchMaxHumanoids = 350,
    WatchMaxEvents = 1800,
    ExportAssets = true,             -- MeshId, TextureId, SoundId, AnimationId
    IncludeAttributes = true,
    IncludeTags = true,
    IncludeOwnCharacter = true,
    IncludeOwnInventory = true,
    IncludeOwnData = true,           -- only local player's ValueBase stats, not other players
    IncludeStarterGui = true,
    IncludeStarterPack = true,
    IncludeReplicatedFirst = true,
    IncludeSoundService = true,
    IncludeLighting = true,
    IncludeTeams = true,
    -- For safety, ModuleScripts are CATALOGUED but NEVER required/executed.
    -- Script/LocalScript.Source and private server objects are NOT accessed.
}

-- Options come from the UI; no scan is started during initial load.
OPTIONS.ObserveSeconds = ctx.observeSeconds or 0
OPTIONS.MaxInstanceRecords = ctx.deepScan and 220000 or 100000
OPTIONS.FolderPrefix = "BF_Atlas_v3"
-- =================== BOOT =====================
local gameRef = game
local Players = gameRef:GetService("Players")
local Workspace = gameRef:GetService("Workspace")
local RS = gameRef:GetService("ReplicatedStorage")
local Http = gameRef:GetService("HttpService")
local CollectionService = gameRef:GetService("CollectionService")
local LocalPlayer = Players.LocalPlayer
local env = (type(getgenv) == "function" and getgenv()) or _G
local WORLD_IDS = {
    [2753915549] = 1, [85211729168715] = 1,
    [4442272183] = 2, [79091703265657] = 2,
    [7449423635] = 3, [100117331123089] = 3
}
local WORLD = WORLD_IDS[gameRef.PlaceId] or "unknown"
local RUN_TAG = os.date("%Y%m%d_%H%M%S") .. "_" .. tostring(Http:GenerateGUID(false)):sub(1,6)
local OUTPUT = OPTIONS.FolderPrefix .. "_Sea" .. tostring(WORLD) .. "_" .. RUN_TAG
local RUN = {alive=not ctx.cancel, started=os.clock(), files={}, counts={}, errors={},
    categories={}, connections={}, scanned=0, truncated=false, rootCounts={},
    hiddenPlayers=0, observed={}, watchedHumanoids=0, old=nil,
    mobTypes={}, spawnTypes={}, deadModels=0, liveModels=0,
    movementEvents={}, movement={samples=0,maxObservedStudsPerSecond=0,
      maxSingleStepStuds=0, largeJumps=0, possibleSnapbacks=0,
      respawns=0, deaths=0, forcedAnchors=0, teleportErrors=0, stateChanges=0},
    movementConnections=0}

if type(env.__BF_GAME_DATA_COLLECTOR) == "table" and type(env.__BF_GAME_DATA_COLLECTOR.Stop) == "function" then
    pcall(env.__BF_GAME_DATA_COLLECTOR.Stop)
end
env.__BF_GAME_DATA_COLLECTOR = RUN

local function warnInfo(s)
    warn("[BF DATA] " .. tostring(s))
end
local function note(s)
    print("[BF DATA] " .. tostring(s))
end
local function recordError(where, err)
    if #RUN.errors>=250 then return end
    RUN.errors[#RUN.errors + 1] = {at=tostring(where), error=tostring(err)}
    if #RUN.errors <= 12 then warnInfo(tostring(where) .. ": " .. tostring(err)) end
end
local function getApi(name)
    if type(env[name]) == "function" then return env[name] end
    if type(_G[name]) == "function" then return _G[name] end
    return nil
end
local WRITE = getApi("writefile")
local MAKEFOLDER = getApi("makefolder")
local ISFOLDER = getApi("isfolder")
local CLIP = getApi("setclipboard") or getApi("toclipboard")
local canSave = WRITE ~= nil
if canSave and MAKEFOLDER then
    local ok, err = pcall(function()
        if not ISFOLDER or not ISFOLDER(OUTPUT) then MAKEFOLDER(OUTPUT) end
    end)
    if not ok then recordError("create output folder",err); canSave=false end
end
if canSave and not MAKEFOLDER then
    -- Some executors can write only to existing folders: use flat filenames.
    OUTPUT = OPTIONS.FolderPrefix .. "_Sea" .. tostring(WORLD) .. "_" .. RUN_TAG
end

local function filePath(basename)
    if MAKEFOLDER then return OUTPUT .. "/" .. basename end
    return OUTPUT .. "_" .. basename
end
local function safeWrite(name, value)
    if not canSave then return false end
    local ok, json = pcall(Http.JSONEncode, Http, value)
    if not ok then recordError("json: "..name,json); return false end
    local path = filePath(name)
    local wrote, err = pcall(WRITE,path,json)
    if not wrote then recordError("writefile: "..path,err); return false end
    RUN.files[#RUN.files+1] = {file=path, bytes=#json, kind=name}
    ctx.filesWritten = #RUN.files
    return true
end
local buffers = {}
local chunkSeq = {}
local categoryPreview = {}
local function flushCategory(kind)
    local b = buffers[kind]
    if not b or #b == 0 then return end
    chunkSeq[kind] = (chunkSeq[kind] or 0) + 1
    local filename = string.format("%s_%04d.json",kind,chunkSeq[kind])
    safeWrite(filename,b)
    buffers[kind] = {}
end
local function emit(kind, row)
    RUN.counts[kind] = (RUN.counts[kind] or 0) + 1
    if not categoryPreview[kind] then categoryPreview[kind]=row end
    if not canSave then return end
    local b = buffers[kind]
    if not b then b={}; buffers[kind]=b end
    b[#b+1] = row
    if #b >= OPTIONS.ChunkSize then flushCategory(kind) end
end
local function safeValue(fn, fallback)
    local ok, result = pcall(fn)
    if ok then return result end
    return fallback
end
local function shortPath(inst)
    return safeValue(function() return inst:GetFullName() end, tostring(inst.Name))
end
local function v3(v)
    if not v then return nil end
    return {x=math.floor(v.X*100+0.5)/100,
            y=math.floor(v.Y*100+0.5)/100,
            z=math.floor(v.Z*100+0.5)/100}
end
local function cfData(cf)
    if not cf then return nil end
    local parts={cf:GetComponents()}
    for i,n in ipairs(parts) do parts[i]=math.floor(n*10000+0.5)/10000 end
    return {position=v3(cf.Position), components=parts}
end
local function compactString(s, n)
    s = tostring(s)
    if #s <= (n or 2000) then return s end
    -- Avoid cutting in the middle of a UTF-8 character when possible.
    local cut = n or 2000
    local ok, u = pcall(function() return utf8.offset(s, 1, cut) end)
    if ok and type(u)=="number" then cut=math.min(cut,u-1) end
    return s:sub(1,cut) .. " [truncated]"
end
local function serial(v, depth, visited)
    depth = depth or 0
    visited = visited or {}
    if depth > 4 then return "[max-depth]" end
    local kind = typeof(v)
    if kind=="nil" then return nil end
    if kind=="string" then return compactString(v,1500) end
    if kind=="boolean" then return v end
    if kind=="number" then
        if v~=v or v==math.huge or v==-math.huge then return tostring(v) end
        return v
    end
    if kind=="Vector3" then return {type="Vector3",value=v3(v)} end
    if kind=="Vector2" then return {type="Vector2",x=v.X,y=v.Y} end
    if kind=="CFrame" then return {type="CFrame",value=cfData(v)} end
    if kind=="Color3" then return {type="Color3",r=v.R,g=v.G,b=v.B} end
    if kind=="UDim2" then return {type="UDim2",xs=v.X.Scale,xo=v.X.Offset,ys=v.Y.Scale,yo=v.Y.Offset} end
    if kind=="UDim" then return {type="UDim",scale=v.Scale,offset=v.Offset} end
    if kind=="NumberRange" then return {type="NumberRange",min=v.Min,max=v.Max} end
    if kind=="BrickColor" then return tostring(v) end
    if kind=="EnumItem" then return tostring(v) end
    if kind=="Instance" then return {type="Instance",class=v.ClassName,path=shortPath(v)} end
    if kind=="table" then
        if visited[v] then return "[circular]" end
        visited[v]=true
        local out={}
        local n=0
        for key,value in pairs(v) do
            n=n+1
            if n>100 then out.__truncated="100 entries";break end
            local k=type(key)=="string" and key or tostring(key)
            out[k]=serial(value,depth+1,visited)
        end
        visited[v]=nil
        return out
    end
    return "["..tostring(kind).."]"
end

-- =================== READ-ONLY SNAPSHOT HELPERS ===================
local function humanoid(model)
    return model and model:FindFirstChildOfClass("Humanoid") or nil
end
local function modelRoot(model)
    if not model then return nil end
    return model:FindFirstChild("HumanoidRootPart") or model.PrimaryPart or model:FindFirstChildWhichIsA("BasePart")
end
local function getProp(obj,key)
    return safeValue(function() return obj[key] end,nil)
end
local function isLiveNPC(model)
    local h=humanoid(model)
    return h and h.Health>0 and h.MaxHealth>0 and modelRoot(model)~=nil
end
local function shallowObj(inst)
    local data={name=inst.Name,class=inst.ClassName,path=shortPath(inst)}
    if OPTIONS.IncludeAttributes then
        local attrs=safeValue(function() return inst:GetAttributes() end,{})
        local result={}
        for k,v in pairs(attrs) do result[tostring(k)]=serial(v) end
        if next(result) then data.attributes=result end
    end
    if OPTIONS.IncludeTags then
        local tags=safeValue(function() return CollectionService:GetTags(inst) end,nil)
        if tags and #tags>0 then data.tags=tags end
    end
    if inst:IsA("BasePart") then
        data.position=v3(inst.Position)
        data.size=v3(inst.Size)
        data.cframe=cfData(inst.CFrame)
        data.anchored=inst.Anchored
        data.collidable=inst.CanCollide
        data.canTouch=inst.CanTouch
        data.transparency=inst.Transparency
        data.material=tostring(inst.Material)
        if inst:IsA("MeshPart") then
            data.meshId=getProp(inst,"MeshId")
            data.textureId=getProp(inst,"TextureID")
        end
    elseif inst:IsA("Model") then
        data.pivot=safeValue(function() return cfData(inst:GetPivot()) end,nil)
        data.primaryPart=inst.PrimaryPart and shortPath(inst.PrimaryPart) or nil
        local h=humanoid(inst)
        if h then
            data.humanoid={health=h.Health,maxHealth=h.MaxHealth,alive=h.Health>0,
                displayName=h.DisplayName,walkSpeed=h.WalkSpeed}
        end
    elseif inst:IsA("Humanoid") then
        data.health=inst.Health
        data.maxHealth=inst.MaxHealth
        data.walkSpeed=inst.WalkSpeed
        data.displayName=inst.DisplayName
    elseif inst:IsA("ValueBase") then
        data.value=serial(getProp(inst,"Value"))
    elseif inst:IsA("Tool") then
        data.tooltip=compactString(getProp(inst,"ToolTip") or "",300)
        data.canBeDropped=getProp(inst,"CanBeDropped")
        data.requiresHandle=getProp(inst,"RequiresHandle")
    elseif inst:IsA("ProximityPrompt") then
        data.enabled=inst.Enabled
        data.actionText=inst.ActionText
        data.objectText=inst.ObjectText
        data.holdDuration=inst.HoldDuration
        data.maxActivationDistance=inst.MaxActivationDistance
        data.requiresLineOfSight=inst.RequiresLineOfSight
    elseif inst:IsA("ClickDetector") then
        data.maxActivationDistance=inst.MaxActivationDistance
    elseif inst:IsA("Animation") then
        data.animationId=inst.AnimationId
    elseif inst:IsA("Sound") then
        data.soundId=inst.SoundId
        data.volume=inst.Volume
        data.looped=inst.Looped
    elseif inst:IsA("Decal") or inst:IsA("Texture") then
        data.textureId=inst.Texture
    elseif inst:IsA("SpecialMesh") then
        data.meshId=inst.MeshId
        data.textureId=inst.TextureId
    elseif inst:IsA("TextLabel") or inst:IsA("TextButton") then
        -- Deliberately do NOT read UI text in bulk to avoid personal chat data.
        data.visible=inst.Visible
    elseif inst:IsA("RemoteEvent") or inst:IsA("RemoteFunction") or
        inst:IsA("UnreliableRemoteEvent") then
        data.remoteType=inst.ClassName -- never FireServer/InvokeServer!
    elseif inst:IsA("ModuleScript") then
        data.note="Source is not readable by a normal game client; not required."
    end
    return data
end
local function getWorldOrigin()
    return Workspace:FindFirstChild("_WorldOrigin")
end
local function snapshotEnemy(model)
    if not model:IsA("Model") then return nil end
    local h=humanoid(model)
    local root=modelRoot(model)
    local aliveNow=isLiveNPC(model) and true or false
    local group=RUN.mobTypes[model.Name]
    if not group then
        group={name=model.Name,seen=0,alive=0,deadOrIncomplete=0,examplePositions={}}
        RUN.mobTypes[model.Name]=group
    end
    group.seen=group.seen+1
    if aliveNow then group.alive=group.alive+1;RUN.liveModels=RUN.liveModels+1
    else group.deadOrIncomplete=group.deadOrIncomplete+1;RUN.deadModels=RUN.deadModels+1 end
    if root and #group.examplePositions<4 then
        group.examplePositions[#group.examplePositions+1]=v3(root.Position)
    end
    return {name=model.Name,path=shortPath(model),position=root and v3(root.Position) or nil,
        hasHumanoid=h~=nil,health=h and h.Health or nil,
        maxHealth=h and h.MaxHealth or nil,
        alive=aliveNow,
        humanoidState=h and tostring(h:GetState()) or nil,
        isBossByName=string.find(string.lower(model.Name),"boss",1,true)~=nil}
end
local function isOtherPlayerCharacter(inst)
    if not inst:IsA("Model") then return false end
    local p=safeValue(function() return Players:GetPlayerFromCharacter(inst) end,nil)
    return p~=nil and p~=LocalPlayer
end
local function inFolder(inst, folder)
    return folder and inst~=folder and inst:IsDescendantOf(folder)
end
local function matchesName(inst,pattern)
    return string.find(string.lower(inst.Name),pattern)~=nil
end
local function classEventMatch(obj)
    if not (obj:IsA("Model") or obj:IsA("BasePart") or obj:IsA("Folder")) then return false end
    local name=string.lower(obj.Name)
    local terms={"halloween","pumpkin","ghost","event","dungeon","leviathan","terror",
        "sea beast","shark","raid","factory","dragon","kitsune","island","soul reaper",
        "dough king","sweet chalice","elite","rip_indra","tiki","celestial"}
    for _,term in ipairs(terms) do if name:find(term,1,true) then return true end end
    return false
end

-- =================== LIVE QUEST / OWN DATA ===================
local function snapshotSelf()
    if not LocalPlayer then
        emit("my_character",{note="LocalPlayer unavailable"})
        return
    end
    local own={team=LocalPlayer.Team and LocalPlayer.Team.Name or nil,
        characterLoaded=LocalPlayer.Character~=nil,placeId=gameRef.PlaceId,sea=WORLD}
    local character=LocalPlayer.Character
    local root=character and character:FindFirstChild("HumanoidRootPart")
    local h=character and humanoid(character)
    if root then own.position=v3(root.Position);own.cframe=cfData(root.CFrame) end
    if h then own.health=h.Health;own.maxHealth=h.MaxHealth end
    emit("my_character",own)
    if OPTIONS.IncludeOwnInventory then
        for _,container in ipairs({LocalPlayer:FindFirstChild("Backpack"),character}) do
            if container then
                for _,child in ipairs(container:GetChildren()) do
                    if child:IsA("Tool") then
                        emit("inventory_tools",{name=child.Name,path=shortPath(child),
                            container=container==character and "equipped" or "backpack",
                            tooltip=getProp(child,"ToolTip"),attributes=serial(safeValue(function() return child:GetAttributes() end,{}))})
                    end
                end
            end
        end
    end
    if OPTIONS.IncludeOwnData then
        local data=LocalPlayer:FindFirstChild("Data")
        if data then
            local queue={data};local head=1
            while head<=#queue do
                local v=queue[head];head=head+1
                if v:IsA("ValueBase") then
                    emit("my_stats",{name=v.Name,path=shortPath(v),value=serial(getProp(v,"Value"))})
                end
                if head<=250 then
                    for _,ch in ipairs(v:GetChildren()) do queue[#queue+1]=ch end
                end
            end
        else emit("my_stats",{note="LocalPlayer.Data not loaded"}) end
    end
end
local function snapshotQuestUI()
    local pg=LocalPlayer and LocalPlayer:FindFirstChild("PlayerGui")
    local main=pg and pg:FindFirstChild("Main")
    local quest=main and (main:FindFirstChild("Quest") or main:FindFirstChild("QuestContainer"))
    if not quest then
        emit("quest_ui",{found=false,note="No quest GUI loaded at scan time"})
        return
    end
    emit("quest_ui",{found=true,name=quest.Name,path=shortPath(quest),visible=getProp(quest,"Visible")})
    local total=0
    for _,ch in ipairs(quest:GetDescendants()) do
        if ch:IsA("TextLabel") or ch:IsA("TextButton") then
            total=total+1
            if total<=100 then
                emit("quest_ui",{name=ch.Name,path=shortPath(ch),text=compactString(ch.Text,600),
                    visible=ch.Visible})
            end
        end
    end
end

-- =================== NPC EVENT WATCHER =====================
local enemyFolder=Workspace:FindFirstChild("Enemies")
local watchedHumanoids=setmetatable({},{__mode="k"})
local function eventWatch(evt, data)
    if not RUN.alive or #RUN.observed>=OPTIONS.WatchMaxEvents then return end
    RUN.observed[#RUN.observed+1]={elapsed=math.floor((os.clock()-RUN.started)*100)/100,
        event=evt,data=data}
end
local bindEnemy
bindEnemy = function(model)
    if not RUN.alive or RUN.watchedHumanoids>=OPTIONS.WatchMaxHumanoids then return end
    if not model:IsA("Model") then return end
    local h=humanoid(model)
    if not h then
        -- The Humanoid may be inserted after the NPC model appears.
        local retry
        retry=model.ChildAdded:Connect(function(obj)
            if obj:IsA("Humanoid") then
                retry:Disconnect()
                bindEnemy(model)
            end
        end)
        RUN.connections[#RUN.connections+1]=retry
        return
    end
    if watchedHumanoids[h] then return end
    watchedHumanoids[h]=true
    RUN.watchedHumanoids=RUN.watchedHumanoids+1
    local lastHealth=h.Health
    local signal=h.HealthChanged:Connect(function(value)
        if not RUN.alive then return end
        if value<=0 and lastHealth>0 then
            eventWatch("npc_dead",{name=model.Name,path=shortPath(model),position=modelRoot(model) and v3(modelRoot(model).Position) or nil})
        elseif lastHealth<=0 and value>0 then
            eventWatch("npc_revived",{name=model.Name,path=shortPath(model)})
        end
        lastHealth=value
    end)
    RUN.connections[#RUN.connections+1]=signal
end
-- =================== MOVEMENT / POSSIBLE CORRECTION DIAGNOSTICS ==============
-- This monitors ONLY replicated client state and documented events.
-- It cannot identify server anti-cheat logic or bypass movement validation.
local function movementEvent(kind,data)
    if not RUN.alive or #RUN.movementEvents >= OPTIONS.MovementMaxEvents then return end
    local row={elapsed=math.floor((os.clock()-RUN.started)*100)/100,
        event=kind,data=data or {}}
    RUN.movementEvents[#RUN.movementEvents+1]=row
end
local function installMovementWatch()
    if not OPTIONS.TrackMovementDiagnostics or not LocalPlayer then return end
    local boundChar=setmetatable({},{__mode="k"})
    local function bindCharacter(char)
        if not char or boundChar[char] or not RUN.alive then return end
        boundChar[char]=true
        local hum=char:FindFirstChildOfClass("Humanoid")
        local root=char:FindFirstChild("HumanoidRootPart")
        if hum then
            local died=hum.Died:Connect(function()
                if not RUN.alive then return end
                RUN.movement.deaths=RUN.movement.deaths+1
                movementEvent("own_character_died",{health=hum.Health})
            end)
            RUN.connections[#RUN.connections+1]=died
            local significant={Dead=true,PlatformStanding=true,Ragdoll=true,FallingDown=true,
                Seated=true,Physics=true}
            RUN.connections[#RUN.connections+1]=hum.StateChanged:Connect(function(oldState,newState)
                if not RUN.alive then return end
                local state=tostring(newState):match("%.([^.]+)$") or tostring(newState)
                if significant[state] then
                    RUN.movement.stateChanges=RUN.movement.stateChanges+1
                    movementEvent("humanoid_state",{from=tostring(oldState),to=tostring(newState)})
                end
            end)
        end
        if root then
            RUN.connections[#RUN.connections+1]=root:GetPropertyChangedSignal("Anchored"):Connect(function()
                if RUN.alive then
                    if root.Anchored then RUN.movement.forcedAnchors=RUN.movement.forcedAnchors+1 end
                    movementEvent("root_anchored_changed",{anchored=root.Anchored,
                        position=v3(root.Position)})
                end
            end)
        end
    end
    if LocalPlayer.Character then bindCharacter(LocalPlayer.Character) end
    RUN.connections[#RUN.connections+1]=LocalPlayer.CharacterAdded:Connect(function(char)
        if not RUN.alive then return end
        RUN.movement.respawns=RUN.movement.respawns+1
        movementEvent("character_added",{note="Respawn or character reload"})
        task.defer(function()
            if not RUN.alive then return end
            pcall(function() char:WaitForChild("HumanoidRootPart",5) end)
            if RUN.alive then bindCharacter(char) end
        end)
    end)
    -- Documented teleport failure event. No teleport is requested here.
    pcall(function()
        local TP=gameRef:GetService("TeleportService")
        RUN.connections[#RUN.connections+1]=TP.TeleportInitFailed:Connect(function(player,result,errorMessage,placeId)
            if not RUN.alive or player~=LocalPlayer then return end
            RUN.movement.teleportErrors=RUN.movement.teleportErrors+1
            movementEvent("teleport_init_failed",{result=tostring(result),placeId=placeId,
                reason=compactString(errorMessage or "",220)})
        end)
    end)
    task.spawn(function()
        local prev=nil
        local history={}
        local lastSamplingRecord=0
        local lastRoot=nil
        while RUN.alive do
            local char=LocalPlayer.Character
            local root=char and char:FindFirstChild("HumanoidRootPart")
            local hum=char and char:FindFirstChildOfClass("Humanoid")
            local now=os.clock()
            if root then
                if root~=lastRoot then prev=nil;history={};lastRoot=root end
                local position=root.Position
                if prev then
                    local dt=now-prev.t
                    if dt>=.04 then
                        local distance=(position-prev.pos).Magnitude
                        local speed=distance/dt
                        local M=RUN.movement
                        M.samples=M.samples+1
                        M.maxObservedStudsPerSecond=math.max(M.maxObservedStudsPerSecond,speed)
                        M.maxSingleStepStuds=math.max(M.maxSingleStepStuds,distance)
                        local limit=OPTIONS.LargeMovementStepStuds
                        -- Scale threshold with sample delay so low FPS doesn't create false positives.
                        local adaptiveLimit=math.max(limit,limit*dt/.2)
                        local snapback=false
                        if distance>=40 then
                            for i=#history,1,-1 do
                                local h=history[i]
                                local age=now-h.t
                                if age>6 then break end
                                if age>.7 and (position-h.pos).Magnitude<OPTIONS.ReturnToleranceStuds then
                                    snapback=true
                                    break
                                end
                            end
                        end
                        if distance >= adaptiveLimit then M.largeJumps=M.largeJumps+1 end
                        if snapback or distance >= adaptiveLimit then
                            if snapback then M.possibleSnapbacks=M.possibleSnapbacks+1 end
                            movementEvent(snapback and "possible_snapback" or "large_position_step",{
                                from=v3(prev.pos),to=v3(position),stepStuds=math.floor(distance*10)/10,
                                sampleSeconds=math.floor(dt*100)/100,approxSpeed=math.floor(speed),
                                humanoidState=hum and tostring(hum:GetState()) or "unknown",
                                note="Possible correction / teleport / lag. Not proof of anti-cheat."})
                        end
                        if now-lastSamplingRecord>=2 then
                            lastSamplingRecord=now
                            movementEvent("movement_sample",{position=v3(position),
                                velocity=v3(root.AssemblyLinearVelocity),
                                anchored=root.Anchored,
                                state=hum and tostring(hum:GetState()) or "unknown"})
                        end
                    end
                end
                prev={pos=position,t=now}
                history[#history+1]={pos=position,t=now}
                if #history>34 then table.remove(history,1) end
            else
                prev=nil
                lastRoot=nil
                history={}
            end
            task.wait(math.max(.12,OPTIONS.MovementSampleInterval))
        end
    end)
end
local function installWatch()
    if OPTIONS.ObserveSeconds<=0 then return end
    installMovementWatch()
    if enemyFolder then
        for _,m in ipairs(enemyFolder:GetChildren()) do bindEnemy(m) end
        RUN.connections[#RUN.connections+1] = enemyFolder.ChildAdded:Connect(function(m)
            eventWatch("enemy_added",{name=m.Name,path=shortPath(m)})
            bindEnemy(m)
        end)
        RUN.connections[#RUN.connections+1] = enemyFolder.ChildRemoved:Connect(function(m)
            eventWatch("enemy_removed",{name=m.Name,path=shortPath(m)})
        end)
    end
    -- Only subscribe to specifically named quest remote events. Never fire a remote.
    for _,obj in ipairs(RS:GetDescendants()) do
        if obj:IsA("RemoteEvent") then
            local lname=string.lower(obj.Name)
            if lname=="questupdate" or lname=="questchanged" or
                lname=="queststatus" or lname=="updatequest" then
                RUN.connections[#RUN.connections+1] = obj.OnClientEvent:Connect(function(...)
                    local args={...}
                    eventWatch("quest_remote",{path=shortPath(obj),arguments=serial(args)})
                end)
            end
        end
    end
end
local function stop()
    if not RUN.alive then return end
    RUN.alive=false
    for _,c in ipairs(RUN.connections) do pcall(function() c:Disconnect() end) end
    RUN.connections={}
end
RUN.Stop=stop
ctx.stop=stop
if ctx.cancel then stop() end

-- =================== INSTANCE TREE SCANNER ===================
local origin=getWorldOrigin()
local enemySpawn=origin and origin:FindFirstChild("EnemySpawns")
local playerSpawn=origin and origin:FindFirstChild("PlayerSpawns")
local locations=origin and origin:FindFirstChild("Locations")
local map=Workspace:FindFirstChild("Map")
local npcs=Workspace:FindFirstChild("NPCs")
local boats=Workspace:FindFirstChild("Boats")
-- =================== AREA / ISLAND ATLAS ===================
-- Anchors are derived ONLY from replicated map objects / WorldOrigin.Locations.
-- Proximity assignment is heuristic. It is NOT a verified gameplay region boundary.
local REGIONS={}
local LOC_ANCHORS={}
local MAP_ANCHORS={}
local MAP_REGION={}
local REGION_CAPS={enemies=180,npcs=180,enemySpawns=260,playerSpawns=90,
    interactions=240,landmarks=160,boats=100,mapObjects=100}
local function getSpatialPosition(obj)
    if not obj then return nil end
    if obj:IsA("BasePart") then return obj.Position end
    if obj:IsA("Attachment") then return obj.WorldPosition end
    if obj:IsA("Model") then
        return safeValue(function() return obj:GetPivot().Position end,nil)
    end
    if obj:IsA("Folder") then
        local child=safeValue(function() return obj:FindFirstChildWhichIsA("BasePart",true) end,nil)
        if child then return child.Position end
        local model=safeValue(function() return obj:FindFirstChildWhichIsA("Model",true) end,nil)
        if model then return getSpatialPosition(model) end
    end
    return nil
end
local function makeRegion(obj,source,pos)
    if not obj or not pos or #REGIONS>=140 then return nil end
    if source=="Location" and #LOC_ANCHORS>=70 then return nil end
    if source=="Map" and #MAP_ANCHORS>=70 then return nil end
    local id=#REGIONS+1
    local reg={id=id,name=obj.Name,source=source,path=shortPath(obj),
        center=v3(pos),counts={},details={},dropped={},partsSeen=0}
    REGIONS[id]=reg
    local anchor={entry=reg,position=pos}
    if source=="Location" then LOC_ANCHORS[#LOC_ANCHORS+1]=anchor
    else MAP_ANCHORS[#MAP_ANCHORS+1]=anchor end
    return reg
end
-- A catchall region prevents data from vanishing if no island is replicated.
local UNMAPPED={id=0,name="Unmapped / Ocean",source="Fallback",path="",center=nil,
    counts={},details={},dropped={},partsSeen=0}
local function buildRegions()
    if locations then
        for i,child in ipairs(locations:GetChildren()) do
            if not RUN.alive then break end
            local pos=getSpatialPosition(child)
            if pos then makeRegion(child,"Location",pos) end
            if i%20==0 then task.wait() end
        end
    end
    if map then
        local children=map:GetChildren()
        for i,child in ipairs(children) do
            if not RUN.alive then break end
            -- Models/Folders define structural islands more reliably than loose parts.
            if child:IsA("Model") or child:IsA("Folder") then
                local pos=getSpatialPosition(child)
                if pos then MAP_REGION[child]=makeRegion(child,"Map",pos) end
            end
            if i%16==0 then task.wait() end
        end
        if #MAP_ANCHORS==0 then
            -- Some maps contain top-level Parts only; use a capped fallback.
            for i,child in ipairs(children) do
                if not RUN.alive or #MAP_ANCHORS>=40 then break end
                if child:IsA("BasePart") then MAP_REGION[child]=makeRegion(child,"Map",child.Position) end
                if i%30==0 then task.wait() end
            end
        end
    end
    ctx.regionCount=#REGIONS
end
local function mapParentRegion(obj)
    if not map then return nil end
    local cur=obj
    local guard=0
    while cur and cur~=map and guard<32 do
        if cur.Parent==map then return MAP_REGION[cur] end
        cur=cur.Parent;guard=guard+1
    end
    return nil
end
local function regionFor(obj,position,preferMap)
    if preferMap then
        local exact=mapParentRegion(obj)
        if exact then return exact end
    end
    local pos=position or getSpatialPosition(obj)
    if not pos then return UNMAPPED end
    local chosen,best=nil,math.huge
    local candidates=(#LOC_ANCHORS>0) and LOC_ANCHORS or MAP_ANCHORS
    for _,anchor in ipairs(candidates) do
        local delta=pos-anchor.position
        local d2=delta.X*delta.X+delta.Y*delta.Y+delta.Z*delta.Z
        if d2<best then best=d2;chosen=anchor.entry end
    end
    if chosen and best<=5000*5000 then return chosen end
    -- Do not assign distant ocean objects to an unrelated island.
    return UNMAPPED
end
local function trackRegion(kind,obj,row,preferMap)
    if not obj then return end
    local pos=getSpatialPosition(obj)
    local reg=regionFor(obj,pos,preferMap)
    reg.counts[kind]=(reg.counts[kind] or 0)+1
    local limit=REGION_CAPS[kind] or 80
    local bucket=reg.details[kind]
    if not bucket then bucket={};reg.details[kind]=bucket end
    -- The per-region JSON contains previews; this chunked category stores EVERY
    -- matched object with a region ID (no per-region preview limit).
    emit("region_members",{regionId=reg.id,regionName=reg.name,regionPath=reg.path,
        kind=kind,record=row})
    if #bucket<limit then
        bucket[#bucket+1]={name=obj.Name,class=obj.ClassName,path=shortPath(obj),
            position=row and (row.position or (row.pivot and row.pivot.position)) or v3(pos),
            health=row and row.health or nil,alive=row and row.alive or nil,
            enabled=row and row.enabled or nil,actionText=row and row.actionText or nil}
    else reg.dropped[kind]=(reg.dropped[kind] or 0)+1 end
end
local function regionSummaries()
    local entries={}
    for _,r in ipairs(REGIONS) do entries[#entries+1]=r end
    entries[#entries+1]=UNMAPPED
    local index={}
    for _,r in ipairs(entries) do
        local file=string.format("region_%03d.json",r.id)
        local snapshot={id=r.id,name=r.name,source=r.source,path=r.path,center=r.center,
            counts=r.counts,geometry={partCount=r.partsSeen,bounds=r.bounds},
            examples=r.details,additionalRecordsInTree=RUN.counts.tree_Workspace or 0,
            cappedExamples=r.dropped,
            note="Nearest island/location is estimated; read tree_Workspace_*.json for full replicated object records."}
        safeWrite(file,snapshot)
        local total=0
        for _,n in pairs(r.counts) do total=total+n end
        index[#index+1]={id=r.id,name=r.name,path=r.path,center=r.center,source=r.source,
            counts=r.counts,geometry={partCount=r.partsSeen,bounds=r.bounds},
            totalMatched=total,detailsFile=file}
    end
    safeWrite("regions_index.json",{sea=WORLD,placeId=gameRef.PlaceId,
        regions=index,replicatedRegionCount=#REGIONS,
        allIslandsGuaranteed=false,
        classification="Nearest available Locations anchor or Map top-level ancestry; approximate",
        streamingWarning="Unloaded islands/objects cannot be scanned from client. Re-scan after travelling."})
    ctx.regionsIndex=index
    ctx.regionCount=#REGIONS
    return index
end
local function scan(root,label)
    if not root or not RUN.alive then return end
    local q={[1]=root}
    local head,tail=1,1
    local scannedThisRoot=0
    while head<=tail and RUN.alive do
        if RUN.scanned>=OPTIONS.MaxInstanceRecords then RUN.truncated=true;break end
        local obj=q[head];q[head]=nil;head=head+1
        if obj and obj.Parent or obj==root then
            if not isOtherPlayerCharacter(obj) then
                RUN.scanned=RUN.scanned+1
                scannedThisRoot=scannedThisRoot+1
                local row=safeValue(function() return shallowObj(obj) end,nil)
                if not row then
                    recordError("instance read: "..shortPath(obj),"Object became unavailable during scan")
                    row={name=obj.Name,class=obj.ClassName,path=shortPath(obj),error="read failed"}
                end
                emit("tree_"..label,row)

                if obj:IsA("RemoteEvent") or obj:IsA("RemoteFunction") or
                    obj:IsA("UnreliableRemoteEvent") then
                    emit("remotes",row)
                elseif obj:IsA("ModuleScript") then
                    emit("modules",{name=obj.Name,path=shortPath(obj),note="not executed"})
                    if obj.Name:lower():find("quest",1,true) or obj.Name:lower():find("guide",1,true) then
                        emit("quest_sources",{name=obj.Name,path=shortPath(obj),kind="ModuleScript metadata"})
                    end
                elseif obj:IsA("Tool") then
                    emit("tools",row)
                elseif obj:IsA("ProximityPrompt") or obj:IsA("ClickDetector") then
                    emit("interactions",row)
                elseif OPTIONS.ExportAssets and (obj:IsA("Animation") or obj:IsA("Sound") or
                    obj:IsA("Decal") or obj:IsA("Texture") or obj:IsA("SpecialMesh") or obj:IsA("MeshPart")) then
                    emit("assets",row)
                end

                if label=="Workspace" then
                    if obj:IsA("Model") and enemyFolder and obj.Parent==enemyFolder then
                        local mob=snapshotEnemy(obj)
                        emit("enemies",mob)
                        trackRegion("enemies",obj,mob,false)
                    end
                    if npcs and inFolder(obj,npcs) and obj:IsA("Model") then
                        local h=humanoid(obj)
                        if h or obj.Parent==npcs then
                            local npc={name=obj.Name,path=shortPath(obj),pivot=row.pivot,
                                hasHumanoid=h~=nil,alive=isLiveNPC(obj) and true or false}
                            emit("npcs",npc)
                            trackRegion("npcs",obj,npc,false)
                        end
                    end
                    if inFolder(obj,enemySpawn) and obj:IsA("BasePart") then
                        local group=RUN.spawnTypes[obj.Name]
                        if not group then
                            group={name=obj.Name,count=0,examplePositions={}}
                            RUN.spawnTypes[obj.Name]=group
                        end
                        group.count=group.count+1
                        if #group.examplePositions<5 then group.examplePositions[#group.examplePositions+1]=row.position end
                        emit("enemy_spawns",{name=obj.Name,path=shortPath(obj),position=row.position,cframe=row.cframe})
                        trackRegion("enemySpawns",obj,row,false)
                    end
                    if inFolder(obj,playerSpawn) and obj:IsA("BasePart") then
                        emit("player_spawns",{name=obj.Name,path=shortPath(obj),position=row.position})
                        trackRegion("playerSpawns",obj,row,false)
                    end
                    if locations and obj.Parent==locations then
                        emit("locations",{name=obj.Name,path=shortPath(obj),position=row.position or
                            (row.pivot and row.pivot.position),cframe=row.cframe or row.pivot})
                    end
                    if map and obj.Parent==map then
                        emit("map_top_level",{name=obj.Name,class=obj.ClassName,path=shortPath(obj),
                            position=row.position or (row.pivot and row.pivot.position)})
                    end
                    if boats and inFolder(obj,boats) and
                        (obj:IsA("Model") or obj:IsA("VehicleSeat")) then
                        emit("boats",row)
                        trackRegion("boats",obj,row,false)
                    end
                    if obj:IsA("ProximityPrompt") or obj:IsA("ClickDetector") then
                        local positionOwner=obj.Parent or obj
                        trackRegion("interactions",positionOwner,row,false)
                    end
                    if classEventMatch(obj) then
                        emit("events_and_landmarks",row)
                        trackRegion("landmarks",obj,row,false)
                    end
                    if map and inFolder(obj,map) and obj:IsA("BasePart") then
                        local mapRegion=mapParentRegion(obj) or UNMAPPED
                        mapRegion.partsSeen=mapRegion.partsSeen+1
                        local p=obj.Position
                        if not mapRegion.bounds then
                            mapRegion.bounds={min=v3(p),max=v3(p)}
                        else
                            local mn,mx=mapRegion.bounds.min,mapRegion.bounds.max
                            mn.x=math.min(mn.x,p.X);mn.y=math.min(mn.y,p.Y);mn.z=math.min(mn.z,p.Z)
                            mx.x=math.max(mx.x,p.X);mx.y=math.max(mx.y,p.Y);mx.z=math.max(mx.z,p.Z)
                        end
                    end
                    if map and obj.Parent==map and (obj:IsA("Model") or obj:IsA("BasePart") or obj:IsA("Folder")) then
                        trackRegion("mapObjects",obj,row,true)
                    end
                end

                local children=safeValue(function() return obj:GetChildren() end,{})
                for _,ch in ipairs(children) do tail=tail+1;q[tail]=ch end
                if RUN.scanned%OPTIONS.YieldEvery==0 then
                    ctx.scanned=RUN.scanned
                    ctx.mobs=RUN.liveModels+RUN.deadModels
                    if ctx.update then ctx.update("Quét "..label.." · "..tostring(RUN.scanned).." đối tượng",nil) end
                    task.wait()
                end
            else
                RUN.hiddenPlayers=RUN.hiddenPlayers+1
            end
        end
    end
    RUN.rootCounts[label]=scannedThisRoot
    note(label .. " -> " .. scannedThisRoot .. " objects")
end

-- =================== OUTPUT / MANIFEST ==================
local function makeManifest()
    local mobTypeCount=0
    for _ in pairs(RUN.mobTypes) do mobTypeCount=mobTypeCount+1 end
    local catNames={}
    for kind,count in pairs(RUN.counts) do catNames[#catNames+1]={category=kind,records=count,chunks=chunkSeq[kind] or 0} end
    table.sort(catNames,function(a,b) return a.category<b.category end)
    return {
        project="Blox Fruits Game Data Collector",
        version="3.0.0",
        snapshotDateUTC=os.date("!%Y-%m-%dT%H:%M:%SZ"),
        placeId=gameRef.PlaceId,
        gameId=gameRef.GameId,
        jobId=gameRef.JobId,
        sea=WORLD,
        durationSeconds=math.floor((os.clock()-RUN.started)*100)/100,
        clientStreamingWarning="Workspace scan may omit unloaded distant locations",
        serverOnlyWarning="No access to ServerScriptService/ServerStorage, private code or server-only state",
        modulesWarning="ModuleScript names only. No module source read or require executed",
        noNetworkRequests=true,
        noRemoteCalls=true,
        antiCheatBypass=false,
        telemetryOnly=true,
        antiCheatWarning="Movement jumps, snapbacks or disconnections are observations only, not proof of server anti-cheat actions",
        movementSummary=RUN.movement,
        areaAtlas={regionCount=#REGIONS,unmappedCounts=UNMAPPED.counts,
            regionsIndexFile="regions_index.json",areaCoverageIsComplete=false},
        multiSeaIndexFile="BF_Atlas_v3_MultiSea.json",
        scanStoppedEarly=ctx.cancel or false,
        ownDataOnly=true,
        truncated=RUN.truncated,
        maxInstanceRecords=OPTIONS.MaxInstanceRecords,
        objectsScanned=RUN.scanned,
        otherPlayerCharactersSkipped=RUN.hiddenPlayers,
        liveEnemyModelsAtScan=RUN.liveModels,
        deadOrIncompleteEnemyModelsAtScan=RUN.deadModels,
        uniqueEnemyTypes=mobTypeCount,
        rootCounts=RUN.rootCounts,
        categoryStats=catNames,
        files=RUN.files,
        errors=RUN.errors,
        settings={observeSeconds=OPTIONS.ObserveSeconds,chunkSize=OPTIONS.ChunkSize,
            yieldEvery=OPTIONS.YieldEvery,includeTags=OPTIONS.IncludeTags},
    }
end
local function finalise()
    if ctx.update then ctx.update("Đang tổng hợp khu vực và xuất JSON...",97) end
    local areas=regionSummaries()
    local mobSummary={}
    for _,entry in pairs(RUN.mobTypes) do mobSummary[#mobSummary+1]=entry end
    table.sort(mobSummary,function(a,b) return a.name<b.name end)
    local spawnSummary={}
    for _,entry in pairs(RUN.spawnTypes) do spawnSummary[#spawnSummary+1]=entry end
    table.sort(spawnSummary,function(a,b) return a.name<b.name end)
    safeWrite("mob_summary.json",mobSummary)
    safeWrite("enemy_spawn_summary.json",spawnSummary)
    safeWrite("movement_summary.json",RUN.movement)
    if #RUN.movementEvents>0 then
        for _,r in ipairs(RUN.movementEvents) do emit("movement_events",r) end
    end
    for kind in pairs(buffers) do flushCategory(kind) end
    if #RUN.observed>0 then
        for _,r in ipairs(RUN.observed) do emit("observations",r) end
        flushCategory("observations")
    end
    local manifest=makeManifest()
    -- manifest should list all category data files; summary file itself is separate.
    safeWrite("manifest.json",manifest)
    local summary={
        "BLOX FRUITS DATA COLLECTOR 3.0 — AREA ATLAS",
        "Sea: "..tostring(WORLD).." | PlaceId: "..tostring(gameRef.PlaceId),
        "Captured objects: "..tostring(RUN.scanned),
        "Truncated: "..tostring(RUN.truncated),
        "Export path: "..OUTPUT,
        "JSON files written: "..tostring(#RUN.files),
        "Known replicated regions: "..tostring(#REGIONS),
        "Regions data: regions_index.json + region_*.json",
        "Quest/UI update events: see observations_*.json",
        "Movement diagnostics: see movement_summary.json and movement_events_*.json",
        "Potential snapbacks: "..tostring(RUN.movement.possibleSnapbacks),
        "NOTE: Only client-replicated content from the current Sea.",
        "Re-run in Sea 1 / Sea 2 / Sea 3 and near distant islands for wider coverage."
    }
    for _,s in ipairs(summary) do note(s) end
    if not canSave then
        warnInfo("No writefile support. Full JSON cannot be saved in this executor.")
        if CLIP then
            local ok=pcall(CLIP,Http:JSONEncode({summary=summary,stats=manifest.categoryStats,
                preview=categoryPreview,errors=RUN.errors}))
            if ok then note("Summary copied to clipboard") end
        end
    else
        local ok,err=pcall(WRITE,filePath("SUMMARY.txt"),table.concat(summary,"\n"))
        if not ok then recordError("SUMMARY.txt",err) end
        note("Saved to: "..OUTPUT)
    end
end

-- =================== MULTI-SEA PERSISTENT INDEX ===================
local function saveMultiSeaIndex()
    local indexFile="BF_Atlas_v3_MultiSea.json"
    local READ=getApi("readfile")
    local ISFILE=getApi("isfile")
    local index={version="3.0.0",mode=ctx.multiSea and "3_sea_workflow" or "local_sea",
        seas={},updatedUTC=os.date("!%Y-%m-%dT%H:%M:%SZ"),
        note="Only records actually observed on client; not a verified list of all islands."}
    if READ then
        local ok,old=pcall(function()
            if ISFILE and not ISFILE(indexFile) then return nil end
            return Http:JSONDecode(READ(indexFile))
        end)
        if ok and type(old)=="table" then
            index.seas=type(old.seas)=="table" and old.seas or {}
            if old.mode=="3_sea_workflow" then index.mode=old.mode end
        end
    end
    local key=tostring(WORLD)
    local previous=index.seas[key] or {}
    local observed=type(previous.observedRegions)=="table" and previous.observedRegions or {}
    for _,r in ipairs(REGIONS) do
        observed[r.path]={name=r.name,source=r.source,center=r.center,
            counts=r.counts,geometryPartCount=r.partsSeen,
            latestFolder=OUTPUT,latestScanUTC=os.date("!%Y-%m-%dT%H:%M:%SZ")}
    end
    index.seas[key]={placeId=gameRef.PlaceId,lastScanFolder=OUTPUT,
        lastScanUTC=index.updatedUTC,runCount=(tonumber(previous.runCount) or 0)+1,
        observedRegions=observed,lastObjectsScanned=RUN.scanned,
        lastInterrupted=ctx.cancel or RUN.truncated,
        note="Visited/replicated locations only; this does not imply every island was fully scanned."}
    index.seaVisits=0
    for _,id in ipairs({"1","2","3"}) do
        if index.seas[id] and (index.seas[id].runCount or 0)>0 then
            index.seaVisits=index.seaVisits+1
        end
    end
    if canSave and WRITE then
        local ok,err=pcall(WRITE,indexFile,Http:JSONEncode(index))
        if not ok then recordError("multi-sea index",err) end
    end
    ctx.catalog=index
    return index
end
-- =================== MAIN ====================
local function main()
    note("Starting read-only snapshot. World/Sea=" .. tostring(WORLD))
    if not LocalPlayer then
        recordError("start","LocalPlayer is missing")
        return
    end
    if ctx.update then ctx.update("Đang dựng danh mục đảo / khu vực...",5) end
    buildRegions()
    if ctx.update then ctx.update("Đang lấy thông tin người chơi và quest...",9) end
    installWatch()
    snapshotSelf()
    snapshotQuestUI()

    -- Read replicated modules/remotes before the potentially huge Workspace tree.
    local roots={
        {"ReplicatedStorage",RS},
        {"Workspace",Workspace},
    }
    if OPTIONS.IncludeReplicatedFirst then
        roots[#roots+1]={"ReplicatedFirst",gameRef:GetService("ReplicatedFirst")}
    end
    if OPTIONS.IncludeLighting then roots[#roots+1]={"Lighting",gameRef:GetService("Lighting")} end
    if OPTIONS.IncludeStarterGui then roots[#roots+1]={"StarterGui",gameRef:GetService("StarterGui")} end
    if OPTIONS.IncludeStarterPack then roots[#roots+1]={"StarterPack",gameRef:GetService("StarterPack")} end
    if OPTIONS.IncludeSoundService then roots[#roots+1]={"SoundService",gameRef:GetService("SoundService")} end
    if OPTIONS.IncludeTeams then roots[#roots+1]={"Teams",gameRef:GetService("Teams")} end
    if OPTIONS.IncludeOwnCharacter and LocalPlayer.Character then
        roots[#roots+1]={"MyCharacter",LocalPlayer.Character}
    end
    for i,tuple in ipairs(roots) do
        if not RUN.alive or RUN.truncated then break end
        if ctx.update then ctx.update("Đang quét "..tuple[1], 10+math.floor(76*(i-1)/#roots)) end
        local ok,err=pcall(scan,tuple[2],tuple[1])
        if not ok then recordError("scan "..tuple[1],err) end
    end
    if RUN.alive and OPTIONS.ObserveSeconds>0 then
        local deadline=os.clock()+OPTIONS.ObserveSeconds
        note("Observation: waiting "..tostring(OPTIONS.ObserveSeconds).." seconds for NPC, quest and movement diagnostics")
        while RUN.alive and os.clock()<deadline do
            if ctx.update then ctx.update("Quan sát NPC & chuyển động · còn "..tostring(math.ceil(deadline-os.clock())).."s", 90+math.floor(6*(1-(deadline-os.clock())/OPTIONS.ObserveSeconds))) end
            task.wait(.4)
        end
    end
end
local success,err=pcall(main)
if not success then recordError("main",err) end
stop()
local ok,finishErr=pcall(finalise)
if not ok then recordError("finalise",finishErr);warnInfo("Export failed: "..tostring(finishErr)) end
local okIndex,indexOrErr=pcall(saveMultiSeaIndex)
if not okIndex then recordError("multi-sea",indexOrErr) end
ctx.scanned=RUN.scanned
ctx.regionCount=#REGIONS
ctx.mobs=RUN.liveModels+RUN.deadModels
ctx.output=OUTPUT
ctx.partial=(ctx.cancel or RUN.truncated or not success or not ok or not canSave)
ctx.errorCount=#RUN.errors
ctx.success=success and ok and okIndex
if ctx.update then
    local message=ctx.partial and "Đã lưu dữ liệu một phần" or "Đã lưu dữ liệu Sea "..tostring(WORLD)
    ctx.update(message,100)
end
if env.__BF_GAME_DATA_COLLECTOR==RUN then env.__BF_GAME_DATA_COLLECTOR=nil end
if ctx.done then pcall(ctx.done,ctx) end

end -- runSnapshot

-- =================== MENU / CONTROLLER (STANDALONE) ===================
local Services=game
local UIPlayers=Services:GetService("Players")
local UITween=Services:GetService("TweenService")
local UIHttp=Services:GetService("HttpService")
local UIPlayer=UIPlayers.LocalPlayer or UIPlayers.PlayerAdded:Wait()
local UIEnv=(type(getgenv)=="function" and getgenv()) or _G
--// CRYSTAL WHITE LIQUID GLASS  |  UI ONLY — collector engine remains unchanged.
--// Inspiration: andepzai v17's monochrome, translucent glass design.
local UIColor={
    white=Color3.fromRGB(255,255,255),ice=Color3.fromRGB(249,250,252),
    pearl=Color3.fromRGB(240,242,245),ink=Color3.fromRGB(28,30,35),
    sub=Color3.fromRGB(89,94,103),quiet=Color3.fromRGB(126,132,141),
    accent=Color3.fromRGB(45,48,54),ok=Color3.fromRGB(37,104,78),
    warn=Color3.fromRGB(151,99,34),line=Color3.fromRGB(223,226,232),
    blue=Color3.fromRGB(232,235,241),danger=Color3.fromRGB(145,55,55),
}
local GUI_STATE={running=false,task=nil,observeSeconds=35,deepScan=true,
    multiSea=false,visible=true,finished=0,catalog=nil,output="",cancel=false}
if type(UIEnv.__BF_ATLAS_GUI_CLEANUP)=="function" then pcall(UIEnv.__BF_ATLAS_GUI_CLEANUP) end
local target=UIPlayer:WaitForChild("PlayerGui",10)
if not target then warn("[BF Atlas] PlayerGui unavailable") return end
local screen=Instance.new("ScreenGui")
screen.Name="BF_Atlas_CrystalWhite_v4"
screen.IgnoreGuiInset=true
screen.ResetOnSpawn=false
screen.DisplayOrder=7800
screen.ZIndexBehavior=Enum.ZIndexBehavior.Sibling
screen.Parent=target
local connections={}
local function bind(c) if c then connections[#connections+1]=c end return c end
local function motion(obj,props,duration,style)
    if not obj or not obj.Parent then return end
    local ok,tw=pcall(function()
        return UITween:Create(obj,TweenInfo.new(duration or .2,style or Enum.EasingStyle.Quad,Enum.EasingDirection.Out),props)
    end)
    if ok and tw then tw:Play() end
end
local function round(obj,r)
    local c=Instance.new("UICorner",obj)
    c.CornerRadius=UDim.new(0,r or 12)
    return c
end
local function stroke(obj,color,thick,alpha)
    local s=Instance.new("UIStroke",obj)
    s.Color=color or UIColor.white;s.Thickness=thick or 1
    s.Transparency=alpha or 0;s.ApplyStrokeMode=Enum.ApplyStrokeMode.Border
    return s
end
local function frame(parent,name,size,pos,bg,r,trans)
    local f=Instance.new("Frame",parent)
    f.Name=name or "Frame";f.Size=size;f.Position=pos or UDim2.new()
    f.BackgroundColor3=bg or UIColor.white
    f.BackgroundTransparency=trans or 0;f.BorderSizePixel=0
    if r then round(f,r) end
    return f
end
local function textLabel(parent,name,txt,pos,size,fontSize,bold,color)
    local t=Instance.new("TextLabel",parent)
    t.Name=name or "Text";t.BackgroundTransparency=1
    t.Text=txt or "";t.Position=pos;t.Size=size
    t.TextColor3=color or UIColor.ink;t.TextSize=fontSize or 13
    t.Font=bold and Enum.Font.GothamBold or Enum.Font.Gotham
    t.TextXAlignment=Enum.TextXAlignment.Left;t.TextYAlignment=Enum.TextYAlignment.Center
    t.TextWrapped=true
    return t
end
local function glass(obj,kind)
    local isRoot=kind=="window"
    if isRoot then
        obj.BackgroundColor3=UIColor.white
        obj.BackgroundTransparency=.25
    elseif kind=="card" then
        obj.BackgroundColor3=UIColor.white
        obj.BackgroundTransparency=.31
    elseif kind=="chip" then
        obj.BackgroundColor3=UIColor.white
        obj.BackgroundTransparency=.39
    else
        obj.BackgroundColor3=UIColor.white
        obj.BackgroundTransparency=.43
    end
    local g=Instance.new("UIGradient",obj)
    g.Rotation=125
    g.Color=ColorSequence.new({
        ColorSequenceKeypoint.new(0,Color3.fromRGB(255,255,255)),
        ColorSequenceKeypoint.new(.35,Color3.fromRGB(246,247,249)),
        ColorSequenceKeypoint.new(.66,Color3.fromRGB(255,255,255)),
        ColorSequenceKeypoint.new(1,Color3.fromRGB(236,238,242)),
    })
    stroke(obj,UIColor.white,isRoot and 1.8 or 1.1,isRoot and .04 or .20)
    local spec=frame(obj,"GlassSpecular",UDim2.new(1,-26,0,2),UDim2.fromOffset(13,2),UIColor.white,2,.3)
    spec.Active=false
    local grad=Instance.new("UIGradient",spec)
    grad.Transparency=NumberSequence.new({NumberSequenceKeypoint.new(0,1),
        NumberSequenceKeypoint.new(.17,.14),NumberSequenceKeypoint.new(.7,.52),
        NumberSequenceKeypoint.new(1,1)})
    return obj
end
-- Hand-drawn monochrome icons; no images, downloads or external UI library.
local function icon(parent,name,pos,size,color)
    local root=frame(parent,"Icon_"..name,UDim2.fromOffset(22,22),pos,UIColor.white,nil,1)
    root.Active=false
    local scale=Instance.new("UIScale",root);scale.Scale=(size or 22)/22
    color=color or UIColor.ink
    local function bar(x,y,w,h,rot)
        local p=frame(root,"Line",UDim2.fromOffset(w,h),UDim2.fromOffset(x,y),color,8)
        p.Rotation=rot or 0;p.Active=false
        return p
    end
    local function line(x,y,x2,y2,t)
        local d=x2-x;local e=y2-y;local len=math.sqrt(d*d+e*e)
        local angle=math.deg(math.atan2(e,d))
        return bar((x+x2-len)/2,(y+y2-(t or 1.7))/2,len,t or 1.7,angle)
    end
    local function ring(x,y,w,h)
        local a=frame(root,"Ring",UDim2.fromOffset(w,h),UDim2.fromOffset(x,y),UIColor.white,math.min(w,h)/2,1)
        stroke(a,color,1.7,0)
    end
    if name=="atlas" then
        line(4,6,10,4);line(10,4,16,6);line(16,6,19,4)
        line(4,6,4,18);line(10,4,10,17);line(16,6,16,18);line(19,4,19,16)
        line(4,18,10,16);line(10,16,16,18);line(16,18,19,16)
    elseif name=="play" then
        line(7,4,17,11,2.3);line(17,11,7,18,2.3);line(7,18,7,4,2.3)
    elseif name=="layers" then
        line(3,8,11,4);line(11,4,19,8);line(19,8,11,12);line(11,12,3,8)
        line(3,12,11,16);line(11,16,19,12)
        line(3,16,11,20);line(11,20,19,16)
    elseif name=="stop" then
        for i=0,1 do bar(6+i*7,5,4,12) end
    elseif name=="radar" then
        ring(4,4,14,14);line(11,11,19,4);bar(9.5,9.5,3,3)
    elseif name=="deep" then
        ring(4,4,14,14);line(11,2,11,6);line(11,16,11,20)
        line(2,11,6,11);line(16,11,20,11);ring(9,9,4,4)
    elseif name=="copy" then
        bar(6,5,11,14);root.Line.BackgroundTransparency=1
        stroke(root.Line,color,1.6,0);bar(3,8,2,11)
    elseif name=="map" then
        ring(6,3,11,11);line(11.5,13,11.5,20)
        line(6,12,11.5,20);line(17,12,11.5,20);ring(9,6,5,5)
    elseif name=="minimize" then
        bar(5,10,12,2)
    elseif name=="back" then
        line(14,5,7,11);line(7,11,14,17);bar(7,10,13,2)
    elseif name=="check" then
        line(4,11,9,16,2.5);line(9,16,19,5,2.5)
    elseif name=="activity" then
        line(2,12,6,12);line(6,12,9,5);line(9,5,13,18)
        line(13,18,16,10);line(16,10,20,10)
    else
        ring(4,4,14,14);bar(10,7,2,8)
    end
    return root
end
local function button(parent,name,txt,pos,size,primary,iconName)
    local b=Instance.new("TextButton",parent)
    b.Name=name;b.Text=txt;b.Position=pos;b.Size=size
    b.Font=Enum.Font.GothamBold;b.TextSize=11.7
    b.TextColor3=primary and UIColor.white or UIColor.ink
    b.TextXAlignment=Enum.TextXAlignment.Center
    b.AutoButtonColor=false
    b.BackgroundColor3=primary and UIColor.accent or UIColor.white
    b.BackgroundTransparency=primary and .03 or .33
    b.BorderSizePixel=0;round(b,13)
    local outline=stroke(b,primary and UIColor.accent or UIColor.white,1.1,primary and .30 or .1)
    -- Put reflection on a separate decorative strip so the button text stays legible.
    local spec=frame(b,"ButtonSpecular",UDim2.new(1,-18,0,2),
        UDim2.fromOffset(9,2),UIColor.white,2,primary and .71 or .39)
    spec.Active=false
    local specGrad=Instance.new("UIGradient",spec)
    specGrad.Transparency=NumberSequence.new({NumberSequenceKeypoint.new(0,1),
        NumberSequenceKeypoint.new(.22,.2),NumberSequenceKeypoint.new(.78,.58),
        NumberSequenceKeypoint.new(1,1)})
    local scale=Instance.new("UIScale",b)
    scale.Scale=1
    if iconName then icon(b,iconName,UDim2.fromOffset(9,math.max(3,math.floor((size.Y.Offset-17)/2))),17,
        primary and UIColor.white or UIColor.accent) end
    bind(b.MouseEnter:Connect(function()
        motion(scale,{Scale=1.025},.16)
        motion(b,{BackgroundTransparency=primary and 0 or .17},.18)
        motion(outline,{Transparency=primary and .12 or 0},.18)
    end))
    bind(b.MouseLeave:Connect(function()
        motion(scale,{Scale=1},.16)
        motion(b,{BackgroundTransparency=primary and .03 or .33},.18)
        motion(outline,{Transparency=primary and .30 or .1},.18)
    end))
    bind(b.Activated:Connect(function()
        motion(scale,{Scale=.965},.08)
        task.delay(.105,function()if b.Parent then motion(scale,{Scale=1},.22,Enum.EasingStyle.Back)end end)
    end))
    return b
end
local WIDTH,HEIGHT=548,552
local glow=frame(screen,"DiffuseGlow",UDim2.fromOffset(WIDTH+20,HEIGHT+20),
    UDim2.new(.5,-WIDTH/2-5,.5,-HEIGHT/2+5),UIColor.white,35,.92)
local shadow=frame(screen,"SoftShadow",UDim2.fromOffset(WIDTH+12,HEIGHT+12),
    UDim2.new(.5,-WIDTH/2+2,.5,-HEIGHT/2+9),Color3.fromRGB(20,24,30),32,.80)
local wrap=frame(screen,"Container",UDim2.fromOffset(WIDTH,HEIGHT),UDim2.fromScale(.5,.5),UIColor.white,28,.25)
wrap.AnchorPoint=Vector2.new(.5,.5)
wrap.ClipsDescendants=true
glass(wrap,"window")
local sz=Instance.new("UIScale",wrap)
glow.AnchorPoint=Vector2.new(.5,.5)
shadow.AnchorPoint=Vector2.new(.5,.5)
local function syncDepth()
    glow.Position=UDim2.new(wrap.Position.X.Scale,wrap.Position.X.Offset+2,
        wrap.Position.Y.Scale,wrap.Position.Y.Offset+4)
    shadow.Position=UDim2.new(wrap.Position.X.Scale,wrap.Position.X.Offset+4,
        wrap.Position.Y.Scale,wrap.Position.Y.Offset+10)
end
local function resize()
    local camera=workspace.CurrentCamera
    local vp=camera and camera.ViewportSize or Vector2.new(800,600)
    sz.Scale=math.clamp(math.min((vp.X-18)/WIDTH,(vp.Y-28)/HEIGHT,1),.47,1)
    glow.Size=UDim2.fromOffset((WIDTH+20)*sz.Scale,(HEIGHT+20)*sz.Scale)
    shadow.Size=UDim2.fromOffset((WIDTH+12)*sz.Scale,(HEIGHT+12)*sz.Scale)
    syncDepth()
end
resize()
bind(wrap:GetPropertyChangedSignal("Position"):Connect(syncDepth))
if workspace.CurrentCamera then bind(workspace.CurrentCamera:GetPropertyChangedSignal("ViewportSize"):Connect(resize)) end
-- Optional low-strength scene blur, removed when this script is closed.
local ownedBlur=Instance.new("BlurEffect")
ownedBlur.Name="BF_Atlas_CrystalBackdrop"
ownedBlur.Size=0
ownedBlur.Parent=game:GetService("Lighting")
local function setBackdrop(on)motion(ownedBlur,{Size=on and 4 or 0},.22)end
setBackdrop(true)
local top=frame(wrap,"Header",UDim2.new(1,-28,0,76),UDim2.fromOffset(14,10),UIColor.white,nil,1)
local brandChip=frame(top,"BrandIcon",UDim2.fromOffset(46,46),UDim2.fromOffset(5,4),UIColor.white,16,.25)
glass(brandChip,"chip")
icon(brandChip,"atlas",UDim2.fromOffset(12,12),22)
textLabel(top,"Brand","DATA ATLAS",UDim2.fromOffset(62,2),UDim2.fromOffset(270,32),22,true)
textLabel(top,"Tagline","C R Y S T A L   W H I T E    /    4.0",
    UDim2.fromOffset(63,34),UDim2.fromOffset(245,20),8.6,true,UIColor.sub)
local activeTag=frame(top,"ReadOnly",UDim2.fromOffset(102,23),UDim2.new(1,-212,0,7),UIColor.white,12,.36)
glass(activeTag,"chip")
local tagText=textLabel(activeTag,"Caption","●  READ ONLY",UDim2.new(),UDim2.fromScale(1,1),10,true,UIColor.accent)
tagText.TextXAlignment=Enum.TextXAlignment.Center
local regionBtn=button(top,"Regions","VÙNG",UDim2.new(1,-107,0,7),UDim2.fromOffset(70,30),false,"map")
local close=button(top,"Minimize","−",UDim2.new(1,-32,0,7),UDim2.fromOffset(31,30),false)
close.TextSize=18
local mini=button(screen,"OpenAtlas","BF DATA",UDim2.new(1,-114,.43,0),UDim2.fromOffset(102,42),false,"atlas")
mini.Visible=false
local seaRow=frame(wrap,"SeaRow",UDim2.new(1,-28,0,65),UDim2.fromOffset(14,89),UIColor.white,17,.35)
glass(seaRow,"card")
local seaLabels={}
local seaCards={}
for i=1,3 do
    local x=7+(i-1)*172
    local s=frame(seaRow,"SeaCard"..i,UDim2.fromOffset(164,49),UDim2.fromOffset(x,8),UIColor.white,13,.38)
    glass(s,"chip")
    seaCards[i]=s
    local dot=frame(s,"SeaDot",UDim2.fromOffset(7,7),UDim2.fromOffset(10,21),UIColor.quiet,4)
    local lbl=textLabel(s,"SeaLabel","SEA "..i.."  ·  CHƯA QUÉT",UDim2.fromOffset(22,4),UDim2.fromOffset(139,42),11,true,UIColor.sub)
    seaLabels[i]=lbl
end
textLabel(wrap,"StatusCaption","TRẠNG THÁI THU THẬP",UDim2.fromOffset(20,158),UDim2.fromOffset(240,17),10,true,UIColor.sub)
local status=frame(wrap,"StatusBox",UDim2.new(1,-28,0,103),UDim2.fromOffset(14,180),UIColor.white,17,.33)
glass(status,"card")
local statusDot=frame(status,"StatusDot",UDim2.fromOffset(9,9),UDim2.fromOffset(15,18),UIColor.quiet,5)
local statusTitle=textLabel(status,"Status","SẴN SÀNG — CHƯA BẮT ĐẦU",
    UDim2.fromOffset(31,9),UDim2.new(1,-48,0,28),14,true)
local statusNote=textLabel(status,"Note","Chọn BẮT ĐẦU để quét dữ liệu Sea hiện tại.",
    UDim2.fromOffset(16,39),UDim2.new(1,-32,0,30),11.4,false,UIColor.sub)
local progressTrack=frame(status,"Progress",UDim2.new(1,-32,0,8),
    UDim2.new(0,16,1,-19),UIColor.blue,7,.05)
local progressFill=frame(progressTrack,"Fill",UDim2.fromScale(0,1),UDim2.new(),UIColor.accent,7)
local progressGrad=Instance.new("UIGradient",progressFill)
progressGrad.Color=ColorSequence.new({ColorSequenceKeypoint.new(0,UIColor.accent),
    ColorSequenceKeypoint.new(.52,Color3.fromRGB(117,121,131)),
    ColorSequenceKeypoint.new(1,Color3.fromRGB(54,58,64))})
textLabel(wrap,"StatsCaption","THỐNG KÊ TRỰC TIẾP",UDim2.fromOffset(20,287),UDim2.fromOffset(220,15),10,true,UIColor.sub)
local metricRow=frame(wrap,"LiveMetrics",UDim2.new(1,-28,0,64),UDim2.fromOffset(14,306),UIColor.white,nil,1)
local metricBoxes={}
local metricValues={}
local metricLabels={"ĐỐI TƯỢNG","QUÁI / BOSS","KHU VỰC","FILE JSON"}
for i=1,4 do
    local x=(i-1)*131
    local m=frame(metricRow,"Metric"..i,UDim2.fromOffset(124,61),UDim2.fromOffset(x,0),UIColor.white,14,.33)
    glass(m,"card")
    metricBoxes[i]=m
    metricValues[i]=textLabel(m,"Value","0",UDim2.fromOffset(11,8),UDim2.fromOffset(110,28),21,true)
    textLabel(m,"Caption",metricLabels[i],UDim2.fromOffset(11,36),UDim2.fromOffset(110,16),9.1,true,UIColor.sub)
end
local controls=frame(wrap,"Controls",UDim2.new(1,-28,0,83),UDim2.fromOffset(14,380),UIColor.white,nil,1)
local startBtn=button(controls,"Start","BẮT ĐẦU QUÉT",UDim2.fromOffset(0,0),UDim2.fromOffset(170,39),true,"play")
local allBtn=button(controls,"AllSea","QUÉT 3 SEA",UDim2.fromOffset(179,0),UDim2.fromOffset(170,39),false,"layers")
local stopBtn=button(controls,"Stop","DỪNG / XUẤT",UDim2.fromOffset(358,0),UDim2.fromOffset(162,39),false,"stop")
local optsBtn=button(controls,"Observe","QUAN SÁT: 35s",UDim2.fromOffset(0,47),UDim2.fromOffset(170,34),false,"radar")
local deepBtn=button(controls,"Deep","QUÉT SÂU: BẬT",UDim2.fromOffset(179,47),UDim2.fromOffset(170,34),false,"deep")
local copyBtn=button(controls,"Copy","SAO CHÉP PATH",UDim2.fromOffset(358,47),UDim2.fromOffset(162,34),false,"copy")
local data=frame(wrap,"ExportPath",UDim2.new(1,-28,0,48),UDim2.fromOffset(14,475),UIColor.white,13,.35)
glass(data,"card")
local outputPath=textLabel(data,"Path","ĐƯỜNG DẪN: CHƯA CÓ BẢN QUÉT",UDim2.fromOffset(14,3),UDim2.new(1,-28,0,22),10.6,true)
outputPath.TextTruncate=Enum.TextTruncate.AtEnd
local guide=textLabel(data,"Guide","Chỉ quét dữ liệu client đã được tải · Đi qua khu vực khác và quét lại để mở rộng.",
    UDim2.fromOffset(14,23),UDim2.new(1,-28,0,21),9.5,false,UIColor.sub)
local footer=textLabel(wrap,"Footer","READ-ONLY    •    KHÔNG GỌI REMOTE    •    KHÔNG TELEPORT    •    KHÔNG BYPASS",
    UDim2.fromOffset(14,526),UDim2.new(1,-28,0,17),9,false,UIColor.quiet)
footer.TextXAlignment=Enum.TextXAlignment.Center
-- Region explorer is another view within the same liquid-glass window.
local currentSea
local regionPanel=frame(wrap,"RegionExplorer",UDim2.new(1,-28,0,352),
    UDim2.fromOffset(14,163),UIColor.white,18,.10)
glass(regionPanel,"card")
regionPanel.Visible=false
icon(regionPanel,"map",UDim2.fromOffset(16,12),20)
textLabel(regionPanel,"RegionHeading","BẢN ĐỒ KHU VỰC ĐÃ GHI NHẬN",
    UDim2.fromOffset(45,8),UDim2.fromOffset(300,30),14,true)
local regionClose=button(regionPanel,"CloseRegions","QUAY LẠI",UDim2.new(1,-116,0,10),UDim2.fromOffset(103,28),false,"back")
local regionSeas={}
for i=1,3 do
    regionSeas[i]=button(regionPanel,"RegionSea"..i,"SEA "..i,
        UDim2.fromOffset(13+(i-1)*172,47),UDim2.fromOffset(160,31),false)
end
local regionScroll=Instance.new("ScrollingFrame",regionPanel)
regionScroll.Name="AreaList";regionScroll.Position=UDim2.fromOffset(13,89)
regionScroll.Size=UDim2.new(1,-26,1,-101)
regionScroll.BackgroundTransparency=1
regionScroll.BorderSizePixel=0
regionScroll.ScrollBarThickness=3
regionScroll.ScrollBarImageColor3=UIColor.quiet
regionScroll.ScrollingDirection=Enum.ScrollingDirection.Y
regionScroll.CanvasSize=UDim2.new(0,0,0,0)
local regionLayout=Instance.new("UIListLayout",regionScroll)
regionLayout.Padding=UDim.new(0,6)
regionLayout.SortOrder=Enum.SortOrder.LayoutOrder
local displayedSea=1
local function refreshRegionExplorer(sea)
    displayedSea=sea
    for _,child in ipairs(regionScroll:GetChildren()) do
        if child:IsA("Frame") or child:IsA("TextLabel") then child:Destroy() end
    end
    for i,b in ipairs(regionSeas) do
        motion(b,{BackgroundTransparency=i==sea and .07 or .36},.16)
    end
    local row=GUI_STATE.catalog and GUI_STATE.catalog.seas and GUI_STATE.catalog.seas[tostring(sea)]
    local entries={}
    if row and type(row.observedRegions)=="table" then
        for path,reg in pairs(row.observedRegions) do
            entries[#entries+1]={path=path,name=reg.name or path,counts=reg.counts or {},
                parts=reg.geometryPartCount or 0,source=reg.source or "Unknown"}
        end
        table.sort(entries,function(a,b)return a.name<b.name end)
    end
    if #entries==0 then
        local rowFrame=frame(regionScroll,"Empty",UDim2.new(1,-9,0,68),UDim2.new(),UIColor.white,12,.30)
        glass(rowFrame,"chip")
        textLabel(rowFrame,"Note","SEA "..sea.." CHƯA CÓ DỮ LIỆU\nHãy vào Sea này rồi nhấn BẮT ĐẦU QUÉT.",
            UDim2.fromOffset(14,7),UDim2.new(1,-28,0,55),12,false,UIColor.sub)
        rowFrame.LayoutOrder=1
    end
    for i,r in ipairs(entries) do
        local c=r.counts
        local item=frame(regionScroll,"Region"..i,UDim2.new(1,-9,0,59),UDim2.new(),UIColor.white,12,.34)
        glass(item,"chip")
        textLabel(item,"Name",r.name,UDim2.fromOffset(12,4),UDim2.new(1,-24,0,25),12.1,true)
        local detail=string.format("%s   ·   NPC %d   ·   QUÁI %d   ·   SPAWN %d   ·   MAP %d",
            r.source,c.npcs or 0,c.enemies or 0,c.enemySpawns or 0,r.parts)
        textLabel(item,"Counts",detail,UDim2.fromOffset(12,29),UDim2.new(1,-24,0,23),10.3,false,UIColor.sub)
        item.LayoutOrder=i+1
    end
    regionScroll.CanvasSize=UDim2.fromOffset(0,(#entries>0 and #entries*65 or 74))
end
for i,b in ipairs(regionSeas) do
    bind(b.Activated:Connect(function()refreshRegionExplorer(i)end))
end
bind(regionBtn.Activated:Connect(function()
    refreshRegionExplorer(type(currentSea)=="number" and currentSea or 1)
    regionPanel.Visible=true
    regionPanel.BackgroundTransparency=.8
    motion(regionPanel,{BackgroundTransparency=.10},.22)
end))
bind(regionClose.Activated:Connect(function()
    regionPanel.Visible=false
end))
local function loadCatalog()
    local fn=UIEnv.readfile or _G.readfile
    local isf=UIEnv.isfile or _G.isfile
    if type(fn)~="function" then return nil end
    local ok,doc=pcall(function()
        if type(isf)=="function" and not isf("BF_Atlas_v3_MultiSea.json") then return nil end
        return UIHttp:JSONDecode(fn("BF_Atlas_v3_MultiSea.json"))
    end)
    if ok and type(doc)=="table" then return doc end
    return nil
end
local worldIDs={[2753915549]=1,[85211729168715]=1,[4442272183]=2,[79091703265657]=2,
    [7449423635]=3,[100117331123089]=3}
currentSea=worldIDs[game.PlaceId] or "?"
local function updateSeas()
    GUI_STATE.catalog=loadCatalog() or GUI_STATE.catalog
    for i=1,3 do
        local row=GUI_STATE.catalog and GUI_STATE.catalog.seas and GUI_STATE.catalog.seas[tostring(i)]
        local n=0
        if row and type(row.observedRegions)=="table" then
            for _ in pairs(row.observedRegions) do n=n+1 end
        end
        if row then
            seaLabels[i].Text="SEA "..i.."  ·  "..tostring(n).." VÙNG\n"..tostring(row.runCount or 1).." LƯỢT QUÉT"
            seaLabels[i].TextColor3=UIColor.ok
        elseif currentSea==i then
            seaLabels[i].Text="SEA "..i.."  ·  HIỆN TẠI\nCHƯA LƯU"
            seaLabels[i].TextColor3=UIColor.accent
        else
            seaLabels[i].Text="SEA "..i.."  ·  CHƯA QUÉT"
            seaLabels[i].TextColor3=UIColor.sub
        end
        local dot=seaCards[i]:FindFirstChild("SeaDot")
        if dot then motion(dot,{BackgroundColor3=row and UIColor.ok
            or (currentSea==i and UIColor.accent or UIColor.quiet)},.2) end
    end
end
local function updateMetrics(ctx)
    if not ctx then return end
    local values={ctx.scanned or 0,ctx.mobs or 0,ctx.regionCount or 0,ctx.filesWritten or 0}
    for i,value in ipairs(values) do
        local raw=tostring(value)
        local pretty=raw:reverse():gsub("(%d%d%d)","%1,"):reverse():gsub("^,","")
        metricValues[i].Text=pretty
    end
end
local statusDotColor=nil
local function statusChange(message,pct)
    statusTitle.Text=message
    local state=GUI_STATE.running and UIColor.accent or (pct==100 and UIColor.ok or UIColor.quiet)
    if state~=statusDotColor then
        statusDotColor=state
        motion(statusDot,{BackgroundColor3=state},.2)
    end
    if pct~=nil then
        motion(progressFill,{Size=UDim2.fromScale(math.clamp(pct/100,0,1),1)},.20)
    end
end
local function begin(multi)
    if GUI_STATE.running then statusNote.Text="Đang quét, nhấn DỪNG nếu muốn xuất dữ liệu một phần.";return end
    GUI_STATE.running=true
    GUI_STATE.multiSea=multi
    GUI_STATE.cancel=false
    local ctx={multiSea=multi,observeSeconds=GUI_STATE.observeSeconds,
        deepScan=GUI_STATE.deepScan,cancel=false,scanned=0,mobs=0,regionCount=0,
        filesWritten=0,output="",partial=false,errorCount=0}
    GUI_STATE.task=ctx
    statusNote.Text=multi and "Chế độ 3 Sea: quét Sea hiện tại, đổi Sea và chạy tiếp." or "Đang thu thập thông tin từ Sea hiện tại."
    startBtn.Text="ĐANG QUÉT..."
    allBtn.Text="ĐANG QUÉT..."
    motion(startBtn,{TextTransparency=.35},.16)
    motion(allBtn,{TextTransparency=.35},.16)
    statusChange("ĐANG KHỞI TẠO · SEA "..tostring(currentSea),2)
    ctx.update=function(message,pct)
        if GUI_STATE.task~=ctx or not screen.Parent then return end
        statusChange(message,pct)
        updateMetrics(ctx)
    end
    ctx.done=function(finished)
        if GUI_STATE.task~=finished then return end
        GUI_STATE.running=false
        GUI_STATE.finished=GUI_STATE.finished+1
        GUI_STATE.output=finished.output or ""
        GUI_STATE.catalog=finished.catalog or GUI_STATE.catalog
        updateSeas()
        if regionPanel.Visible then refreshRegionExplorer(displayedSea) end
        updateMetrics(finished)
        outputPath.Text="Đường dẫn: "..(finished.output or "—")
        statusNote.Text=finished.partial and "Có dữ liệu một phần. Hãy đến khu vực khác rồi quét lại." or
            "Đã xuất JSON. Đổi Sea hoặc tới đảo khác và quét tiếp để tăng độ phủ."
        if finished.errorCount>0 then
            statusNote.Text=statusNote.Text.."  |  Lỗi ghi nhận: "..tostring(finished.errorCount)
        end
        startBtn.Text="QUÉT LẠI SEA"
        allBtn.Text="TIẾP TỤC 3 SEA"
        motion(startBtn,{TextTransparency=0},.16)
        motion(allBtn,{TextTransparency=0},.16)
        statusChange(finished.partial and "ĐÃ XUẤT DỮ LIỆU MỘT PHẦN" or "HOÀN TẤT LƯỢT QUÉT",100)
    end
    task.spawn(function()
        local ok,err=pcall(runSnapshot,ctx)
        if not ok then
            warn("[BF Atlas] Fatal collector error: "..tostring(err))
            GUI_STATE.running=false
            startBtn.Text="THỬ QUÉT LẠI"
            allBtn.Text="QUY TRÌNH 3 SEA"
            motion(startBtn,{TextTransparency=0},.16)
            motion(allBtn,{TextTransparency=0},.16)
            statusChange("LỖI: "..tostring(err):sub(1,70),0)
            statusNote.Text="Xem console. Các file ghi trước đó (nếu có) vẫn được giữ."
        end
    end)
end
bind(startBtn.Activated:Connect(function()begin(false)end))
bind(allBtn.Activated:Connect(function()begin(true)end))
bind(stopBtn.Activated:Connect(function()
    local ctx=GUI_STATE.task
    if not GUI_STATE.running or not ctx then
        statusNote.Text="Hiện không có lượt quét đang chạy.";return
    end
    ctx.cancel=true
    if ctx.stop then pcall(ctx.stop) end
    statusChange("ĐANG DỪNG VÀ XUẤT JSON...",96)
end))
bind(optsBtn.Activated:Connect(function()
    local choices={0,20,35,60}
    local idx=1
    for i,v in ipairs(choices) do if v==GUI_STATE.observeSeconds then idx=i;break end end
    GUI_STATE.observeSeconds=choices[idx%#choices+1]
    optsBtn.Text="QUAN SÁT: "..tostring(GUI_STATE.observeSeconds).."s"
    statusNote.Text="Áp dụng cho lượt quét tiếp theo."
end))
bind(deepBtn.Activated:Connect(function()
    GUI_STATE.deepScan=not GUI_STATE.deepScan
    deepBtn.Text=GUI_STATE.deepScan and "QUÉT SÂU: BẬT" or "QUÉT SÂU: TẮT"
    statusNote.Text="Quét sâu: tối đa 220.000 đối tượng, quét thường: 100.000."
end))
bind(copyBtn.Activated:Connect(function()
    local clip=UIEnv.setclipboard or _G.setclipboard or UIEnv.toclipboard or _G.toclipboard
    if type(clip)=="function" and GUI_STATE.output~="" then
        local ok=pcall(clip,GUI_STATE.output)
        statusNote.Text=ok and "Đã sao chép đường dẫn thư mục." or "Không thể sao chép trên executor này."
    elseif GUI_STATE.output~="" then
        statusNote.Text="Đường dẫn được ghi ở ô phía trên, hãy mở bằng trình quản lý file."
    else statusNote.Text="Chưa có kết quả. Hãy quét trước." end
end))
local function setVisible(visible)
    GUI_STATE.visible=visible
    if visible then
        wrap.Visible=true;shadow.Visible=true;glow.Visible=true;mini.Visible=false
        sz.Scale=sz.Scale*.96
        motion(sz,{Scale=math.clamp(sz.Scale/.96,.47,1)},.24,Enum.EasingStyle.Back)
        setBackdrop(true)
    else
        setBackdrop(false)
        shadow.Visible=false;glow.Visible=false;wrap.Visible=false
        mini.Visible=true
        local mScale=mini:FindFirstChildOfClass("UIScale")
        if not mScale then mScale=Instance.new("UIScale",mini) end
        mScale.Scale=.83;motion(mScale,{Scale=1},.22,Enum.EasingStyle.Back)
    end
end
bind(close.Activated:Connect(function()setVisible(false)end))
bind(mini.Activated:Connect(function()setVisible(true)end))
-- Drag only on the heading, never from the VÙNG or MINIMIZE controls.
local UIS=Services:GetService("UserInputService")
local dragging=false
local dragType=nil
local down=nil
local initial=nil
bind(top.InputBegan:Connect(function(input)
    if input.UserInputType~=Enum.UserInputType.MouseButton1
       and input.UserInputType~=Enum.UserInputType.Touch then return end
    if input.Position.X>top.AbsolutePosition.X+top.AbsoluteSize.X-128*sz.Scale then return end
    dragging=true;down=input.Position;initial=wrap.Position;dragType=input.UserInputType
end))
bind(UIS.InputChanged:Connect(function(input)
    if not dragging or not down or not initial then return end
    if input.UserInputType~=Enum.UserInputType.MouseMovement
       and input.UserInputType~=Enum.UserInputType.Touch then return end
    if dragType==Enum.UserInputType.Touch and input.UserInputType~=Enum.UserInputType.Touch then return end
    -- ScreenGui-relative offset is already in screen pixels; do not divide by UIScale.
    local d=input.Position-down
    local vp=workspace.CurrentCamera and workspace.CurrentCamera.ViewportSize or Vector2.new(800,600)
    local nx=math.clamp(initial.X.Offset+d.X,-vp.X*.5+65,vp.X*.5-65)
    local ny=math.clamp(initial.Y.Offset+d.Y,-vp.Y*.5+58,vp.Y*.5-58)
    wrap.Position=UDim2.new(initial.X.Scale,nx,initial.Y.Scale,ny)
end))
bind(UIS.InputEnded:Connect(function(input)
    if input.UserInputType==Enum.UserInputType.MouseButton1
       or input.UserInputType==Enum.UserInputType.Touch then
        dragging=false;down=nil;initial=nil;dragType=nil
    end
end))
UIEnv.__BF_ATLAS_GUI_CLEANUP=function()
    if GUI_STATE.task and GUI_STATE.running then
        GUI_STATE.task.cancel=true
        if GUI_STATE.task.stop then pcall(GUI_STATE.task.stop) end
    end
    for _,conn in ipairs(connections) do pcall(function()conn:Disconnect()end) end
    pcall(function()ownedBlur:Destroy()end)
    pcall(function()screen:Destroy()end)
end
updateSeas()
local loadedScale=sz.Scale
sz.Scale=loadedScale*.94
motion(sz,{Scale=loadedScale},.30,Enum.EasingStyle.Back)
statusNote.Text="Sea hiện tại: "..tostring(currentSea)..". Nhấn BẮT ĐẦU khi sẵn sàng."
print("[BF Atlas v4 / Crystal White] Menu opened. No scan started automatically. Sea="..tostring(currentSea))
